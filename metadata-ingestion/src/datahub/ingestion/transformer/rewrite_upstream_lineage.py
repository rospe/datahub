import copy
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional, Pattern, Tuple, cast

from pydantic import Field, field_validator

from datahub.configuration.common import ConfigModel
from datahub.emitter.aspect import JSON_PATCH_CONTENT_TYPE
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetUpstreamLineageTransformer,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)

# Match a schemaField URN and extract the embedded dataset URN.
# Format: urn:li:schemaField:(<dataset_urn>,<field_path>)
# Note: <field_path> may contain commas — anchor on the last comma before the closing paren.
_SCHEMA_FIELD_URN_PATTERN: Pattern[str] = re.compile(
    r"^(urn:li:schemaField:\()(urn:li:dataset:\(.+\)),(.+)(\))$"
)

_DATASET_URN_PREFIX = "urn:li:dataset:("
_SCHEMA_FIELD_URN_PREFIX = "urn:li:schemaField:("


def _unquote_json_pointer(component: str) -> str:
    """Reverse JSON Pointer escaping (RFC 6901): ~1 -> /, ~0 -> ~.

    Order matters: ``~0`` must be unescaped *after* ``~1`` so that the literal
    sequence ``~01`` round-trips correctly.
    """
    return component.replace("~1", "/").replace("~0", "~")


def _quote_json_pointer(component: str) -> str:
    """Apply JSON Pointer escaping (RFC 6901): ~ -> ~0, / -> ~1.

    Order matters: ``~`` must be escaped before ``/`` so that ``/`` doesn't get
    double-escaped via the introduced ``~``.
    """
    return component.replace("~", "~0").replace("/", "~1")


# How often to emit a progress log line (in number of processed entities).
_PROGRESS_LOG_INTERVAL = 1000


class UpstreamLineageRewriteRule(ConfigModel):
    """A single regex rewrite rule applied to upstream dataset URNs."""

    match: str = Field(
        description="Regex pattern matched against the upstream dataset URN. "
        "Use Python re syntax. Backreferences in `replace` are supported.",
    )
    replace: str = Field(
        description="Replacement string. Supports backreferences (e.g. \\1) "
        "from groups in `match`.",
    )

    @field_validator("match")
    @classmethod
    def _validate_match_compiles(cls, v: str) -> str:
        try:
            re.compile(v)
        except re.error as exc:
            raise ValueError(f"Invalid regex in `match`: {v!r} ({exc})") from exc
        return v


class PatternRewriteUpstreamLineageConfig(ConfigModel):
    rules: List[UpstreamLineageRewriteRule] = Field(
        description="Ordered list of rewrite rules. Rules are applied sequentially "
        "to each upstream dataset URN — each rule's output feeds the next.",
    )


@dataclass
class _RewriteStats:
    entities_processed: int = 0
    upstream_urns_seen: int = 0
    upstream_urns_rewritten: int = 0
    fine_grained_urns_seen: int = 0
    fine_grained_urns_rewritten: int = 0
    invalid_rewrites: int = 0
    patch_ops_processed: int = 0
    patch_ops_rewritten: int = 0
    patch_aspects_processed: int = 0
    patch_aspects_rewritten: int = 0
    compiled_rules: List[Pattern[str]] = field(default_factory=list)


class PatternRewriteUpstreamLineage(DatasetUpstreamLineageTransformer):
    """Rewrite upstream dataset URNs in upstreamLineage aspects using regex rules.

    Rewrites both:
      * coarse-grained upstreams (`upstreamLineage.upstreams[].dataset`)
      * fine-grained upstream field URNs (the dataset URN embedded inside
        `upstreamLineage.fineGrainedLineages[].upstreams[]` schemaField URNs)

    The downstream entity URN is never modified — only the references to upstream
    datasets within the lineage aspect are rewritten.

    Both UPSERT (full-aspect) and PATCH change types are supported. Sources that
    enable `incremental_lineage` (e.g. Snowflake, BigQuery) emit upstreamLineage
    as JSON-patch MCPs via `convert_upstream_lineage_to_patch`; this transformer
    rewrites the embedded URNs in those patch operations as well.

    Useful for bridging URN mismatches between sources. For example, when a
    Snowflake source emits an external S3 upstream URN without a `platform_instance`
    prefix but the S3 source was ingested with one, this transformer can rewrite
    the upstream URN to match.
    """

    ctx: PipelineContext
    config: PatternRewriteUpstreamLineageConfig

    def __init__(
        self, config: PatternRewriteUpstreamLineageConfig, ctx: PipelineContext
    ) -> None:
        super().__init__()
        self.ctx = ctx
        self.config = config
        self._stats = _RewriteStats(
            compiled_rules=[re.compile(rule.match) for rule in config.rules]
        )
        logger.info(
            "PatternRewriteUpstreamLineage initialized with %d rule(s)",
            len(config.rules),
        )
        for idx, rule in enumerate(config.rules, start=1):
            logger.debug(
                "  rule %d: %r -> %r",
                idx,
                rule.match,
                rule.replace,
            )

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternRewriteUpstreamLineage":
        config = PatternRewriteUpstreamLineageConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _rewrite_dataset_urn(self, urn: str) -> str:
        """Apply all rules sequentially to a dataset URN."""
        rewritten = urn
        for compiled, rule in zip(
            self._stats.compiled_rules, self.config.rules, strict=False
        ):
            rewritten = compiled.sub(rule.replace, rewritten)

        if rewritten == urn:
            return urn

        if not rewritten.startswith("urn:li:"):
            # A rule produced something that is no longer a URN — keep the original
            # and warn so the user can fix their rules.
            logger.warning(
                "Skipping rewrite producing invalid URN: %r -> %r", urn, rewritten
            )
            self._stats.invalid_rewrites += 1
            return urn

        logger.debug("Rewrote upstream URN: %s -> %s", urn, rewritten)
        return rewritten

    def _rewrite_schema_field_urn(self, schema_field_urn: str) -> str:
        """Rewrite the dataset URN embedded inside a schemaField URN."""
        m = _SCHEMA_FIELD_URN_PATTERN.match(schema_field_urn)
        if not m:
            logger.debug(
                "schemaField URN did not match expected format, skipping: %s",
                schema_field_urn,
            )
            return schema_field_urn

        prefix, dataset_urn, field_path, suffix = m.groups()
        new_dataset_urn = self._rewrite_dataset_urn(dataset_urn)
        if new_dataset_urn == dataset_urn:
            return schema_field_urn
        return f"{prefix}{new_dataset_urn},{field_path}{suffix}"

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        if aspect is None:
            return aspect

        in_lineage = cast(UpstreamLineageClass, aspect)
        out_lineage: UpstreamLineageClass = copy.deepcopy(in_lineage)

        # Coarse-grained upstreams.
        for upstream in out_lineage.upstreams or []:
            self._stats.upstream_urns_seen += 1
            new_urn = self._rewrite_dataset_urn(upstream.dataset)
            if new_urn != upstream.dataset:
                upstream.dataset = new_urn
                self._stats.upstream_urns_rewritten += 1

        # Fine-grained upstreams (column-level lineage).
        for fg in out_lineage.fineGrainedLineages or []:
            if not fg.upstreams:
                continue
            new_upstreams: List[str] = []
            for fg_urn in fg.upstreams:
                self._stats.fine_grained_urns_seen += 1
                new_fg_urn = self._rewrite_schema_field_urn(fg_urn)
                if new_fg_urn != fg_urn:
                    self._stats.fine_grained_urns_rewritten += 1
                new_upstreams.append(new_fg_urn)
            fg.upstreams = new_upstreams

        self._stats.entities_processed += 1
        if self._stats.entities_processed % _PROGRESS_LOG_INTERVAL == 0:
            logger.info(
                "PatternRewriteUpstreamLineage progress: %d entities processed, "
                "%d/%d coarse upstreams rewritten, %d/%d fine-grained upstreams rewritten",
                self._stats.entities_processed,
                self._stats.upstream_urns_rewritten,
                self._stats.upstream_urns_seen,
                self._stats.fine_grained_urns_rewritten,
                self._stats.fine_grained_urns_seen,
            )

        return cast(Aspect, out_lineage)

    # --- PATCH MCP support -------------------------------------------------
    #
    # Sources with `incremental_lineage=True` (Snowflake, BigQuery, …) wrap the
    # upstreamLineage aspect into JSON-patch MCPs via
    # `auto_incremental_lineage` / `convert_upstream_lineage_to_patch` BEFORE
    # the transformer chain runs. Those records arrive as raw
    # MetadataChangeProposalClass instances, which `BaseTransformer.transform`
    # passes through untouched (it only dispatches MCEs and MCPWs to
    # `transform_aspect`). To make the rewrite effective for incremental
    # lineage, we override `transform()` and rewrite URNs directly in the
    # patch payload — same approach used by `set_attribution.py`.

    def _rewrite_patch_path(self, path: str) -> Tuple[str, Optional[str]]:
        """Rewrite the last component of a JSON-Pointer path if it is a URN.

        Returns ``(new_path, classification)`` where ``classification`` is one of:
          - ``"coarse"``  — last component is a dataset URN
          - ``"fine"``    — last component is a schemaField URN
          - ``None``      — last component is neither (no rewrite attempted)

        The patch shapes produced by ``DatasetPatchBuilder.add_upstream_lineage``
        and ``add_fine_grained_lineage`` always carry the URN as the *last*
        path component, so this heuristic is precise:

          coarse: ``/upstreams/<dataset_urn>``
          fine:   ``/fineGrainedLineages/<transformOp>/<downstream>/<query>/<upstream>``

        For the fine path the downstream URN appears at index 2 — we deliberately
        leave it alone (rewrites only apply to upstreams, mirroring the UPSERT
        path's behaviour).
        """
        if not path.startswith("/"):
            return path, None

        components = path.split("/")
        last_idx = len(components) - 1
        last = _unquote_json_pointer(components[last_idx])

        if last.startswith(_DATASET_URN_PREFIX):
            new = self._rewrite_dataset_urn(last)
            if new != last:
                components[last_idx] = _quote_json_pointer(new)
                return "/".join(components), "coarse"
            return path, "coarse"

        if last.startswith(_SCHEMA_FIELD_URN_PREFIX):
            new = self._rewrite_schema_field_urn(last)
            if new != last:
                components[last_idx] = _quote_json_pointer(new)
                return "/".join(components), "fine"
            return path, "fine"

        return path, None

    def _rewrite_patch_value(self, value: Any) -> Tuple[Any, bool]:
        """Walk a patch op value and rewrite embedded dataset URNs.

        Currently rewrites:
          - any object's ``dataset`` field that holds a dataset URN string
            (matches the UpstreamClass payload of an ``add /upstreams/<urn>`` op)

        Recurses into nested dicts and lists so list-shaped values (e.g. produced
        by ``set_upstream_lineages``) are handled too. Returns the (possibly new)
        value and a boolean indicating whether anything changed.
        """
        if isinstance(value, dict):
            changed = False
            new_dict: dict = {}
            for k, v in value.items():
                if (
                    k == "dataset"
                    and isinstance(v, str)
                    and v.startswith(_DATASET_URN_PREFIX)
                ):
                    new_v: Any = self._rewrite_dataset_urn(v)
                    if new_v != v:
                        changed = True
                else:
                    new_v, sub_changed = self._rewrite_patch_value(v)
                    changed = changed or sub_changed
                new_dict[k] = new_v
            return new_dict, changed
        if isinstance(value, list):
            changed = False
            new_list: list = []
            for v in value:
                new_v, sub_changed = self._rewrite_patch_value(v)
                changed = changed or sub_changed
                new_list.append(new_v)
            return new_list, changed
        return value, False

    def _rewrite_patch_op(self, op: dict) -> Tuple[dict, bool]:
        """Rewrite a single patch op dict. Returns (new_op, changed)."""
        new_op = dict(op)
        changed = False

        path = op.get("path")
        classification: Optional[str] = None
        if isinstance(path, str):
            new_path, classification = self._rewrite_patch_path(path)
            if classification == "coarse":
                self._stats.upstream_urns_seen += 1
            elif classification == "fine":
                self._stats.fine_grained_urns_seen += 1
            if new_path != path:
                new_op["path"] = new_path
                changed = True

        if "value" in op:
            new_value, value_changed = self._rewrite_patch_value(op["value"])
            if value_changed:
                new_op["value"] = new_value
                changed = True

        if changed:
            if classification == "coarse":
                self._stats.upstream_urns_rewritten += 1
            elif classification == "fine":
                self._stats.fine_grained_urns_rewritten += 1
        return new_op, changed

    def _rewrite_patch_payload(self, payload: Any) -> Tuple[Any, bool]:
        """Rewrite a parsed JSON patch payload.

        Two shapes are produced by ``DatasetPatchBuilder.build()``:
          - plain JSON Patch:           a list of op dicts
          - GenericJsonPatch envelope:  ``{"arrayPrimaryKeys": ..., "patch": [...]}``

        Both are handled. Returns (new_payload, changed).
        """
        if isinstance(payload, list):
            changed = False
            new_ops: List[dict] = []
            for op in payload:
                if isinstance(op, dict):
                    self._stats.patch_ops_processed += 1
                    new_op, op_changed = self._rewrite_patch_op(op)
                    if op_changed:
                        self._stats.patch_ops_rewritten += 1
                    changed = changed or op_changed
                    new_ops.append(new_op)
                else:
                    new_ops.append(op)
            return new_ops if changed else payload, changed

        if isinstance(payload, dict) and isinstance(payload.get("patch"), list):
            new_inner, changed = self._rewrite_patch_payload(payload["patch"])
            if changed:
                new_payload = dict(payload)
                new_payload["patch"] = new_inner
                return new_payload, True
            return payload, False

        return payload, False

    def _rewrite_patch_mcp(
        self, mcp: MetadataChangeProposalClass
    ) -> MetadataChangeProposalClass:
        """Return a new MCP with URNs in its JSON-patch aspect rewritten.

        On any decode/parse error the original MCP is returned unchanged so the
        ingest doesn't fail because of a transformer issue.
        """
        aspect = mcp.aspect
        if not isinstance(aspect, GenericAspectClass):
            return mcp

        if aspect.contentType != JSON_PATCH_CONTENT_TYPE:
            return mcp

        try:
            payload = json.loads(aspect.value.decode())
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning(
                "Could not decode JSON patch payload for %s; passing through unchanged: %s",
                mcp.entityUrn,
                exc,
            )
            return mcp

        self._stats.patch_aspects_processed += 1
        new_payload, changed = self._rewrite_patch_payload(payload)
        if not changed:
            return mcp

        self._stats.patch_aspects_rewritten += 1

        new_aspect = GenericAspectClass(
            value=json.dumps(new_payload).encode(),
            contentType=aspect.contentType,
        )
        new_mcp = MetadataChangeProposalClass(
            entityUrn=mcp.entityUrn,
            entityType=mcp.entityType,
            changeType=mcp.changeType,
            aspectName=mcp.aspectName,
            aspect=new_aspect,
            auditHeader=mcp.auditHeader,
            systemMetadata=mcp.systemMetadata,
        )
        logger.debug("Rewrote PATCH upstreamLineage MCP for entity %s", mcp.entityUrn)
        return new_mcp

    def transform(
        self, record_envelopes: Iterable[RecordEnvelope]
    ) -> Iterable[RecordEnvelope]:
        # Delegate UPSERT (MCE / MCPW) handling to BaseTransformer, which routes
        # them through transform_aspect(). Then intercept raw MCP records that
        # carry PATCH operations on upstreamLineage and rewrite the JSON-patch
        # payload directly — BaseTransformer has no dispatch branch for these.
        for envelope in super().transform(record_envelopes):
            record = envelope.record
            if (
                isinstance(record, MetadataChangeProposalClass)
                and record.aspectName == "upstreamLineage"
                and record.changeType == ChangeTypeClass.PATCH
            ):
                new_mcp = self._rewrite_patch_mcp(record)
                if new_mcp is not record:
                    envelope = RecordEnvelope(
                        record=new_mcp, metadata=envelope.metadata
                    )
            yield envelope

    def handle_end_of_stream(self) -> list:  # type: ignore[type-arg]
        logger.info(
            "PatternRewriteUpstreamLineage summary: %d entities processed, "
            "%d/%d coarse upstreams rewritten, %d/%d fine-grained upstreams rewritten, "
            "%d/%d patch aspects rewritten (%d/%d patch ops), "
            "%d invalid rewrites skipped",
            self._stats.entities_processed,
            self._stats.upstream_urns_rewritten,
            self._stats.upstream_urns_seen,
            self._stats.fine_grained_urns_rewritten,
            self._stats.fine_grained_urns_seen,
            self._stats.patch_aspects_rewritten,
            self._stats.patch_aspects_processed,
            self._stats.patch_ops_rewritten,
            self._stats.patch_ops_processed,
            self._stats.invalid_rewrites,
        )
        return []
