import logging
from typing import List

import pytest

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.rewrite_upstream_lineage import (
    PatternRewriteUpstreamLineage,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
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
