import json
import logging
from typing import List

import pytest

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_upstream_lineage_to_patch,
)
from datahub.ingestion.transformer.rewrite_upstream_lineage import (
    PatternRewriteUpstreamLineage,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GenericAspectClass,
    MetadataChangeProposalClass,
    UpstreamClass,
    UpstreamLineageClass,
)

DOWNSTREAM_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
    "cds.prod01_db_cds_edw.landing_zone.mdm_customer_external_table,PROD)"
)

# Snowflake-emitted upstream URN (no platform_instance, full path with legalEntity)
SNOWFLAKE_S3_UPSTREAM = (
    "urn:li:dataset:(urn:li:dataPlatform:s3,"
    "ttgsl-prod-s3-edp-caspian-lake-customer/partitioning=v1/"
    "event=customer-tibco-customer-created-or-updated-event/legalEntity=TUINL,PROD)"
)

# What the S3 source actually ingests (with platform_instance, no legalEntity)
EXPECTED_S3_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:s3,"
    "tui_data_lake.ttgsl-prod-s3-edp-caspian-lake-customer/partitioning=v1/"
    "event=customer-tibco-customer-created-or-updated-event,PROD)"
)

# Two chained rules: (1) inject platform_instance, (2) strip legalEntity suffix
PLATFORM_INSTANCE_RULE = {
    "match": r"urn:li:dataset:\(urn:li:dataPlatform:s3,(?!tui_data_lake\.)([^,]+),(\w+)\)",
    "replace": r"urn:li:dataset:(urn:li:dataPlatform:s3,tui_data_lake.\1,\2)",
}
STRIP_LEGAL_ENTITY_RULE = {
    "match": r"(urn:li:dataset:\(urn:li:dataPlatform:s3,[^,]+?)/legalEntity=[^/,]+(,\w+\))",
    "replace": r"\1\2",
}


def _make_transformer(rules: List[dict]) -> PatternRewriteUpstreamLineage:
    return PatternRewriteUpstreamLineage.create(
        {"rules": rules},
        PipelineContext(run_id="test-rewrite-upstream-lineage"),
    )


def test_rewrite_coarse_grained_upstream_chained_rules() -> None:
    transformer = _make_transformer([PLATFORM_INSTANCE_RULE, STRIP_LEGAL_ENTITY_RULE])

    aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=SNOWFLAKE_S3_UPSTREAM, type=DatasetLineageTypeClass.COPY
            )
        ]
    )

    result = transformer.transform_aspect(
        DOWNSTREAM_DATASET_URN, "upstreamLineage", aspect
    )
    assert isinstance(result, UpstreamLineageClass)
    assert len(result.upstreams) == 1
    assert result.upstreams[0].dataset == EXPECTED_S3_URN


def test_unmatched_urn_passes_through_unchanged() -> None:
    transformer = _make_transformer([PLATFORM_INSTANCE_RULE, STRIP_LEGAL_ENTITY_RULE])

    other_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
    )
    aspect = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=other_urn, type=DatasetLineageTypeClass.COPY)]
    )

    result = transformer.transform_aspect(
        DOWNSTREAM_DATASET_URN, "upstreamLineage", aspect
    )
    assert isinstance(result, UpstreamLineageClass)
    assert result.upstreams[0].dataset == other_urn


def test_rewrite_fine_grained_lineage_dataset_part_only() -> None:
    transformer = _make_transformer([PLATFORM_INSTANCE_RULE, STRIP_LEGAL_ENTITY_RULE])

    upstream_field = make_schema_field_urn(SNOWFLAKE_S3_UPSTREAM, "customer_id")
    downstream_field = make_schema_field_urn(DOWNSTREAM_DATASET_URN, "customer_id")

    aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=SNOWFLAKE_S3_UPSTREAM, type=DatasetLineageTypeClass.COPY
            )
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[upstream_field],
                downstreams=[downstream_field],
            )
        ],
    )

    result = transformer.transform_aspect(
        DOWNSTREAM_DATASET_URN, "upstreamLineage", aspect
    )
    assert isinstance(result, UpstreamLineageClass)
    expected_upstream_field = make_schema_field_urn(EXPECTED_S3_URN, "customer_id")
    assert result.fineGrainedLineages[0].upstreams == [expected_upstream_field]
    # Downstream URN must be untouched.
    assert result.fineGrainedLineages[0].downstreams == [downstream_field]


def test_invalid_rewrite_is_skipped_and_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A rule that produces a non-URN string — the transformer should reject it
    # and keep the original URN.
    bad_rule = {"match": r"^urn:li:dataset:.*$", "replace": "not-a-urn"}
    transformer = _make_transformer([bad_rule])

    aspect = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=SNOWFLAKE_S3_UPSTREAM, type=DatasetLineageTypeClass.COPY
            )
        ]
    )

    with caplog.at_level(logging.WARNING):
        result = transformer.transform_aspect(
            DOWNSTREAM_DATASET_URN, "upstreamLineage", aspect
        )

    assert isinstance(result, UpstreamLineageClass)
    assert result.upstreams[0].dataset == SNOWFLAKE_S3_UPSTREAM
    assert any("invalid URN" in rec.message for rec in caplog.records)


def test_invalid_regex_at_init_raises() -> None:
    # pydantic wraps our ValueError as ValidationError
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        _make_transformer([{"match": "(unclosed", "replace": "x"}])


# --- PATCH MCP path (incremental_lineage=True) -----------------------------


def _build_patch_mcp(
    downstream_urn: str, lineage: UpstreamLineageClass
) -> MetadataChangeProposalClass:
    """Build a PATCH MCP for upstreamLineage exactly the way
    `auto_incremental_lineage` does it for incremental sources."""
    workunit = convert_upstream_lineage_to_patch(
        urn=downstream_urn, aspect=lineage, system_metadata=None
    )
    mcp = workunit.metadata
    assert isinstance(mcp, MetadataChangeProposalClass), (
        f"convert_upstream_lineage_to_patch should produce an MCP, got {type(mcp)}"
    )
    assert mcp.changeType == ChangeTypeClass.PATCH
    return mcp


def _decode_patch(mcp: MetadataChangeProposalClass) -> object:
    assert isinstance(mcp.aspect, GenericAspectClass)
    return json.loads(mcp.aspect.value.decode())


def _run_transform(
    transformer: PatternRewriteUpstreamLineage,
    records: List[object],
) -> List[object]:
    envelopes = [RecordEnvelope(record=r, metadata={}) for r in records]
    return [env.record for env in transformer.transform(envelopes)]


def test_rewrite_patch_mcp_coarse_upstream() -> None:
    transformer = _make_transformer([PLATFORM_INSTANCE_RULE, STRIP_LEGAL_ENTITY_RULE])

    lineage = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=SNOWFLAKE_S3_UPSTREAM, type=DatasetLineageTypeClass.COPY
            )
        ]
    )
    mcp = _build_patch_mcp(DOWNSTREAM_DATASET_URN, lineage)

    [out] = _run_transform(transformer, [mcp])
    assert isinstance(out, MetadataChangeProposalClass)
    assert out.changeType == ChangeTypeClass.PATCH
    assert out.entityUrn == DOWNSTREAM_DATASET_URN

    payload = _decode_patch(out)
    # The patch path is /upstreams/<urn> and the value carries dataset=<urn>.
    # Both must be rewritten to the canonical S3 URN, with the URN inside the
    # path component JSON-Pointer-escaped (parens and colons aren't escape-worthy
    # but slashes within the URN's table path are).
    assert isinstance(payload, list)
    assert len(payload) == 1
    op = payload[0]
    assert op["op"] == "add"
    assert op["value"]["dataset"] == EXPECTED_S3_URN
    # Path's last component (after JSON-Pointer unescape) must match the new URN.
    last_component = op["path"].rsplit("/", 1)[-1]
    last_component = last_component.replace("~1", "/").replace("~0", "~")
    assert last_component == EXPECTED_S3_URN


def test_rewrite_patch_mcp_fine_grained_upstream() -> None:
    transformer = _make_transformer([PLATFORM_INSTANCE_RULE, STRIP_LEGAL_ENTITY_RULE])

    upstream_field = make_schema_field_urn(SNOWFLAKE_S3_UPSTREAM, "customer_id")
    downstream_field = make_schema_field_urn(DOWNSTREAM_DATASET_URN, "customer_id")
    lineage = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=SNOWFLAKE_S3_UPSTREAM, type=DatasetLineageTypeClass.COPY
            )
        ],
        fineGrainedLineages=[
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                upstreams=[upstream_field],
                downstreams=[downstream_field],
            )
        ],
    )
    mcp = _build_patch_mcp(DOWNSTREAM_DATASET_URN, lineage)

    [out] = _run_transform(transformer, [mcp])
    assert isinstance(out, MetadataChangeProposalClass)

    payload = _decode_patch(out)
    # Fine-grained ops are emitted into the same plain JSON Patch list as the
    # coarse upstream ops (no GenericJsonPatch envelope, since
    # add_fine_grained_lineage doesn't set arrayPrimaryKeys).
    assert isinstance(payload, list)
    ops = payload
    expected_upstream = make_schema_field_urn(EXPECTED_S3_URN, "customer_id")

    # Find the op whose path's last component is the schemaField upstream URN.
    fg_ops = []
    for op in ops:
        path = op["path"]
        last = path.rsplit("/", 1)[-1].replace("~1", "/").replace("~0", "~")
        if last.startswith("urn:li:schemaField:"):
            fg_ops.append((op, last))

    assert fg_ops, f"expected at least one fine-grained op, got {ops!r}"
    for _op, last in fg_ops:
        # Upstream schemaField URN must be rewritten; downstream must not appear
        # in the rewriting path components.
        assert last == expected_upstream, (
            f"fine-grained upstream URN not rewritten: {last!r}"
        )

    # Downstream schemaField URN appears intact at index 3 of the path
    # (/fineGrainedLineages/<transformOp>/<downstreamField>/<query>/<upstreamField>)
    # — make sure we didn't accidentally rewrite that.
    sample_path_components = fg_ops[0][0]["path"].split("/")
    downstream_component = (
        sample_path_components[3].replace("~1", "/").replace("~0", "~")
    )
    assert downstream_component == downstream_field


def test_patch_mcp_with_no_matching_urn_passes_through() -> None:
    transformer = _make_transformer([PLATFORM_INSTANCE_RULE, STRIP_LEGAL_ENTITY_RULE])

    other_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
    )
    lineage = UpstreamLineageClass(
        upstreams=[UpstreamClass(dataset=other_urn, type=DatasetLineageTypeClass.COPY)]
    )
    mcp = _build_patch_mcp(DOWNSTREAM_DATASET_URN, lineage)

    [out] = _run_transform(transformer, [mcp])
    payload = _decode_patch(out)
    assert isinstance(payload, list)
    assert payload[0]["value"]["dataset"] == other_urn


def test_patch_mcp_for_other_aspect_is_untouched() -> None:
    """A PATCH MCP for an aspect we don't care about must pass through unchanged."""
    transformer = _make_transformer([PLATFORM_INSTANCE_RULE])

    unrelated_payload = (
        b'[{"op": "add", "path": "/customProperties/foo", "value": "bar"}]'
    )
    mcp = MetadataChangeProposalClass(
        entityUrn=DOWNSTREAM_DATASET_URN,
        entityType="dataset",
        changeType=ChangeTypeClass.PATCH,
        aspectName="datasetProperties",
        aspect=GenericAspectClass(
            value=unrelated_payload,
            contentType="application/json-patch+json",
        ),
    )

    [out] = _run_transform(transformer, [mcp])
    assert isinstance(out, MetadataChangeProposalClass)
    assert isinstance(out.aspect, GenericAspectClass)
    assert out.aspect.value == unrelated_payload


def test_patch_mcp_with_invalid_payload_passes_through(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If the patch payload can't be decoded, we must not crash the pipeline."""
    transformer = _make_transformer([PLATFORM_INSTANCE_RULE])

    bad_mcp = MetadataChangeProposalClass(
        entityUrn=DOWNSTREAM_DATASET_URN,
        entityType="dataset",
        changeType=ChangeTypeClass.PATCH,
        aspectName="upstreamLineage",
        aspect=GenericAspectClass(
            value=b"not json at all",
            contentType="application/json-patch+json",
        ),
    )

    with caplog.at_level(logging.WARNING):
        [out] = _run_transform(transformer, [bad_mcp])

    assert out is bad_mcp


def test_summary_log_reports_patch_stats(caplog: pytest.LogCaptureFixture) -> None:
    transformer = _make_transformer([PLATFORM_INSTANCE_RULE, STRIP_LEGAL_ENTITY_RULE])

    lineage = UpstreamLineageClass(
        upstreams=[
            UpstreamClass(
                dataset=SNOWFLAKE_S3_UPSTREAM, type=DatasetLineageTypeClass.COPY
            )
        ]
    )
    mcp = _build_patch_mcp(DOWNSTREAM_DATASET_URN, lineage)
    _run_transform(transformer, [mcp])

    with caplog.at_level(logging.INFO):
        transformer.handle_end_of_stream()

    summary_messages = [
        rec.message for rec in caplog.records if "summary" in rec.message
    ]
    assert summary_messages, "expected a summary log line"
    summary = summary_messages[-1]
    assert "patch aspects rewritten" in summary
    assert "1/1 patch aspects rewritten" in summary
