import copy
import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional, Pattern, cast

from pydantic import Field, field_validator

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetUpstreamLineageTransformer,
)
from datahub.metadata.schema_classes import UpstreamLineageClass

logger = logging.getLogger(__name__)

# Match a schemaField URN and extract the embedded dataset URN.
# Format: urn:li:schemaField:(<dataset_urn>,<field_path>)
# Note: <field_path> may contain commas — anchor on the last comma before the closing paren.
_SCHEMA_FIELD_URN_PATTERN: Pattern[str] = re.compile(
    r"^(urn:li:schemaField:\()(urn:li:dataset:\(.+\)),(.+)(\))$"
)

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
    compiled_rules: List[Pattern[str]] = field(default_factory=list)


class PatternRewriteUpstreamLineage(DatasetUpstreamLineageTransformer):
    """Rewrite upstream dataset URNs in upstreamLineage aspects using regex rules.

    Rewrites both:
      * coarse-grained upstreams (`upstreamLineage.upstreams[].dataset`)
      * fine-grained upstream field URNs (the dataset URN embedded inside
        `upstreamLineage.fineGrainedLineages[].upstreams[]` schemaField URNs)

    The downstream entity URN is never modified — only the references to upstream
    datasets within the lineage aspect are rewritten.

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

    def handle_end_of_stream(self) -> list:  # type: ignore[type-arg]
        logger.info(
            "PatternRewriteUpstreamLineage summary: %d entities processed, "
            "%d/%d coarse upstreams rewritten, %d/%d fine-grained upstreams rewritten, "
            "%d invalid rewrites skipped",
            self._stats.entities_processed,
            self._stats.upstream_urns_rewritten,
            self._stats.upstream_urns_seen,
            self._stats.fine_grained_urns_rewritten,
            self._stats.fine_grained_urns_seen,
            self._stats.invalid_rewrites,
        )
        return []
