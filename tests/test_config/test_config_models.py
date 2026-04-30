"""Tests for config model helpers."""

import json
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.config.config_models import (
    AuthConfig,
    DefaultsConfig,
    InputConfig,
    LoadingConfig,
    PipelineConfig,
    PreprocessConfig,
    PreprocessorType,
    PythonSDKConfig,
    QueriesConfig,
    RequestFormatConfig,
    RequestFormatType,
    RequestInputConfig,
    ResourceConfig,
    SourceConfig,
    SourceType,
    StreamingConfig,
)
from src.utils.dynamic_values import (
    ComplexDynamicValue,
    DynamicSourceReference,
    DynamicValueType,
    FilterConfig,
)


def test_pipeline_config_get_effective_loading_destinations_respects_enabled_flags() -> None:
    cfg = PipelineConfig(
        config_root=Path("."),
        version="1.0",
        defaults=DefaultsConfig(),
        queries=[],
        sources={
            "enabled_source": SourceConfig(
                enabled=True,
                type="rest_api",
                base_url="https://example.com",
                resources={
                    "posts": ResourceConfig(
                        enabled=True,
                        path="/posts",
                        method="GET",
                        loading={"destination": "gcs", "gcs_bucket": "bucket-a", "prefix": "a/b"},
                    ),
                    "skip_resource": ResourceConfig(
                        enabled=False,
                        path="/skip",
                        method="GET",
                    ),
                },
            ),
            "disabled_source": SourceConfig(
                enabled=False,
                type="rest_api",
                base_url="https://example.com",
                resources={
                    "posts": ResourceConfig(
                        enabled=True,
                        path="/posts",
                        method="GET",
                    )
                },
            ),
        },
    )

    assert cfg.get_effective_loading_destinations() == {"gcs"}


# ---------------------------------------------------------------------------
# InputConfig.format_request_value
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fmt_type, input_value, expected",
    [
        (RequestFormatType.STRING, "hello", "hello"),
        (RequestFormatType.STRING, ["a", "b"], "a"),  # list → first element as str
        (RequestFormatType.INTEGER, "3", 3),
        (RequestFormatType.INTEGER, ["5"], 5),  # list-wrapped scalar
        (RequestFormatType.FLOAT, "1.5", 1.5),
        (RequestFormatType.FLOAT, ["2.0"], 2.0),
        (RequestFormatType.BOOLEAN, 1, True),
        (RequestFormatType.BOOLEAN, 0, False),
        (RequestFormatType.ARRAY, "x", ["x"]),  # scalar → wrapped in list
        (RequestFormatType.ARRAY, [1, 2], [1, 2]),  # list → passthrough
        (RequestFormatType.JSON_STRING, [1, 2], json.dumps([1, 2])),
        (RequestFormatType.JSON_STRING, {"k": "v"}, json.dumps({"k": "v"})),
    ],
)
def test_input_config_format_request_value_parametrized(fmt_type, input_value, expected) -> None:
    cfg = InputConfig(value="placeholder", request_format=RequestFormatConfig(type=fmt_type))
    assert cfg.format_request_value(input_value) == expected


def test_input_config_format_request_value_no_format_passthrough() -> None:
    cfg = InputConfig(value="x")
    assert cfg.format_request_value("anything") == "anything"
    assert cfg.format_request_value(None) is None


# ---------------------------------------------------------------------------
# InputConfig preprocessing — CONCAT
# ---------------------------------------------------------------------------


def test_input_config_preprocess_concat_joins_list_with_separator() -> None:
    cfg = InputConfig(
        value="placeholder",
        request_format=RequestFormatConfig(
            type=RequestFormatType.STRING,
            preprocess=[PreprocessConfig(type=PreprocessorType.CONCAT, separator=",")],
        ),
    )
    assert cfg.format_request_value(["a", "b", "c"]) == "a,b,c"


def test_input_config_preprocess_concat_wraps_scalar_then_joins() -> None:
    cfg = InputConfig(
        value="placeholder",
        request_format=RequestFormatConfig(
            type=RequestFormatType.STRING,
            preprocess=[PreprocessConfig(type=PreprocessorType.CONCAT, separator="-")],
        ),
    )
    # Scalar is wrapped into [scalar] before CONCAT, then joined → single-element result
    assert cfg.format_request_value("x") == "x"


# ---------------------------------------------------------------------------
# InputConfig classification helpers
# ---------------------------------------------------------------------------


def test_input_config_has_source_config_true_and_false() -> None:
    dynamic_cfg = InputConfig(
        value=ComplexDynamicValue(
            type=DynamicValueType.SOURCE,
            source_config=DynamicSourceReference(source="parent", field="id"),
        )
    )
    assert dynamic_cfg.has_source_config() is True
    assert InputConfig(value="static").has_source_config() is False


def test_input_config_has_filter_config_true_and_false() -> None:
    dynamic_with_filter = InputConfig(
        value=ComplexDynamicValue(
            type=DynamicValueType.SOURCE,
            source_config=DynamicSourceReference(
                source="parent",
                field="id",
                filter=FilterConfig(field="status", value_source="active"),
            ),
        )
    )
    assert dynamic_with_filter.has_filter_config() is True

    dynamic_no_filter = InputConfig(
        value=ComplexDynamicValue(
            type=DynamicValueType.SOURCE,
            source_config=DynamicSourceReference(source="parent", field="id"),
        )
    )
    assert dynamic_no_filter.has_filter_config() is False


def test_input_config_is_static_list_true_and_false() -> None:
    assert InputConfig(value=[1, 2, 3]).is_static_list() is True
    assert (
        InputConfig(
            value=ComplexDynamicValue(
                type=DynamicValueType.SOURCE,
                source_config=DynamicSourceReference(source="p", field="id"),
            )
        ).is_static_list()
        is False
    )
    assert InputConfig(value="x").is_static_list() is False


# ---------------------------------------------------------------------------
# ResourceConfig helpers
# ---------------------------------------------------------------------------


def _minimal_resource(**kwargs) -> ResourceConfig:
    return ResourceConfig(method="GET", path="/r", **kwargs)


def test_resource_config_get_inputs_by_location_filters_correctly() -> None:
    res = _minimal_resource(
        request_inputs={
            "q_param": RequestInputConfig(value="v", location="query"),
            "b_param": RequestInputConfig(value="v", location="body"),
            "p_param": RequestInputConfig(value="v", location="path"),
        }
    )
    assert set(res.get_inputs_by_location("query").keys()) == {"q_param"}
    assert set(res.get_inputs_by_location("body").keys()) == {"b_param"}
    assert set(res.get_inputs_by_location("path").keys()) == {"p_param"}


def test_resource_config_get_batch_inputs_includes_only_batched() -> None:
    res = _minimal_resource(
        request_inputs={
            "batched": RequestInputConfig(value=[1, 2, 3], batch_size=2),
            "not_batched": RequestInputConfig(value="x"),
        }
    )
    batch = res.get_batch_inputs()
    assert "batched" in batch
    assert "not_batched" not in batch


def test_resource_config_get_streaming_config_uses_defaults_when_not_set() -> None:
    res = _minimal_resource()
    defaults = StreamingConfig(flush_threshold=50)
    assert res.get_streaming_config(defaults) is defaults


def test_resource_config_get_streaming_config_resource_override_wins() -> None:
    res = _minimal_resource(streaming=StreamingConfig(flush_threshold=5))
    defaults = StreamingConfig(flush_threshold=50)
    assert res.get_streaming_config(defaults).flush_threshold == 5


def test_resource_config_partial_override_merges_with_defaults() -> None:
    res = ResourceConfig(
        method="GET",
        path="/r",
        loading={"write_mode": "append"},
        _defaults={
            "loading": {
                "destination": "gcs",
                "gcs_bucket": "b",
                "prefix": "a/b",
                "write_mode": "overwrite",
            }
        },
    )
    assert res.loading.destination == "gcs"
    assert res.loading.write_mode == "append"
    assert res.loading.gcs_bucket == "b"


# ---------------------------------------------------------------------------
# SourceConfig.validate_source_type
# ---------------------------------------------------------------------------


def test_source_config_rest_api_missing_base_url_raises() -> None:
    with pytest.raises(ValidationError, match="base_url"):
        SourceConfig(
            type=SourceType.REST_API,
            resources={"r": ResourceConfig(method="GET", path="/r")},
        )


def test_source_config_python_sdk_missing_sdk_raises() -> None:
    with pytest.raises(ValidationError, match="sdk"):
        SourceConfig(
            type=SourceType.PYTHON_SDK,
            resources={"r": ResourceConfig(method="GET", path="/r")},
        )


def test_source_config_python_sdk_valid() -> None:
    src = SourceConfig(
        type=SourceType.PYTHON_SDK,
        sdk=PythonSDKConfig(module="mymodule", class_name="MyClass"),
        resources={"r": ResourceConfig(method="GET", path="/r")},
    )
    assert src.sdk.module == "mymodule"


# ---------------------------------------------------------------------------
# PipelineConfig.validate_queries + load_query_file
# ---------------------------------------------------------------------------


def test_pipeline_config_validate_queries_missing_file_raises(tmp_path: Path) -> None:
    (tmp_path / "queries").mkdir()
    with pytest.raises(ValidationError, match=re.escape("missing.sql")):
        PipelineConfig(
            config_root=tmp_path,
            version="1.0",
            defaults=DefaultsConfig(),
            queries=[QueriesConfig(name="q", file="missing.sql")],
            sources={},
        )


def test_pipeline_config_load_query_file_returns_sql_content(tmp_path: Path) -> None:
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "test.sql").write_text("SELECT 1", encoding="utf-8")
    cfg = PipelineConfig(
        config_root=tmp_path,
        version="1.0",
        defaults=DefaultsConfig(),
        queries=[QueriesConfig(name="test", file="test.sql")],
        sources={},
    )
    assert cfg.load_query_file("test") == "SELECT 1"


def test_pipeline_config_load_query_file_missing_name_raises(tmp_path: Path) -> None:
    (tmp_path / "queries").mkdir()
    cfg = PipelineConfig(
        config_root=tmp_path,
        version="1.0",
        defaults=DefaultsConfig(),
        queries=[],
        sources={},
    )
    with pytest.raises(ValueError, match="Query not found"):
        cfg.load_query_file("nonexistent")


# ---------------------------------------------------------------------------
# LoadingConfig — merge_keys validation
# ---------------------------------------------------------------------------


def test_loading_config_merge_mode_requires_merge_keys() -> None:
    with pytest.raises(ValidationError, match="merge_keys"):
        LoadingConfig(destination="local", storage_root="/tmp", write_mode="merge")


def test_loading_config_merge_empty_list_raises() -> None:
    with pytest.raises(ValidationError, match="merge_keys"):
        LoadingConfig(destination="local", storage_root="/tmp", write_mode="merge", merge_keys=[])


# ---------------------------------------------------------------------------
# LoadingConfig.destination_dedup_key and destination_details
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs, expected_key_prefix",
    [
        ({"destination": "s3", "s3_bucket": "my-bucket", "prefix": "a/b"}, ("s3",)),
        ({"destination": "gcs", "gcs_bucket": "my-bucket", "prefix": "a/b"}, ("gcs",)),
        (
            {
                "destination": "azure_blob",
                "azure_container": "c",
                "azure_account": "acc",
                "prefix": "a/b",
            },
            ("azure_blob",),
        ),
        ({"destination": "local", "storage_root": "/tmp"}, ("local",)),
    ],
)
def test_loading_config_destination_dedup_key_returns_tuple(kwargs, expected_key_prefix) -> None:
    cfg = LoadingConfig(**kwargs)
    key = cfg.destination_dedup_key()
    assert isinstance(key, tuple)
    assert key[0] == expected_key_prefix[0]


@pytest.mark.parametrize(
    "kwargs, expected_dest_key",
    [
        ({"destination": "s3", "s3_bucket": "my-bucket", "prefix": "a/b"}, "s3"),
        ({"destination": "gcs", "gcs_bucket": "my-bucket", "prefix": "a/b"}, "gcs"),
        (
            {
                "destination": "azure_blob",
                "azure_container": "c",
                "azure_account": "acc",
                "prefix": "a/b",
            },
            "azure_blob",
        ),
        ({"destination": "local", "storage_root": "/tmp"}, "local"),
    ],
)
def test_loading_config_destination_details_returns_dict(kwargs, expected_dest_key) -> None:
    cfg = LoadingConfig(**kwargs)
    details = cfg.destination_details()
    assert isinstance(details, dict)
    assert details["destination"] == expected_dest_key


# ---------------------------------------------------------------------------
# LoadingConfig bucket alias normalization
# ---------------------------------------------------------------------------


def test_loading_config_gcs_bucket_alias_normalizes() -> None:
    cfg = LoadingConfig(destination="gcs", bucket="my-gcs-bucket", prefix="a/b")
    assert cfg.gcs_bucket == "my-gcs-bucket"
    assert cfg.bucket == "my-gcs-bucket"


def test_loading_config_azure_bucket_alias_normalizes() -> None:
    cfg = LoadingConfig(
        destination="azure_blob", bucket="mycontainer", azure_account="myaccount", prefix="a/b"
    )
    assert cfg.azure_container == "mycontainer"


def test_loading_config_conflicting_bucket_aliases_raises() -> None:
    with pytest.raises(ValidationError, match="bucket and gcs_bucket cannot both be set"):
        LoadingConfig(destination="gcs", bucket="bucket-a", gcs_bucket="bucket-b", prefix="a/b")


def test_loading_config_azure_missing_account_raises() -> None:
    with pytest.raises(ValidationError, match="azure_account"):
        LoadingConfig(destination="azure_blob", azure_container="c", prefix="a/b")


# ---------------------------------------------------------------------------
# InputConfig.format_request_value — json_string with scalar, default return
# ---------------------------------------------------------------------------


def test_input_config_format_request_value_json_string_scalar() -> None:
    cfg = InputConfig(
        value="x", request_format=RequestFormatConfig(type=RequestFormatType.JSON_STRING)
    )
    assert cfg.format_request_value("hello") == "hello"


# ---------------------------------------------------------------------------
# AuthConfig validators
# ---------------------------------------------------------------------------


def test_auth_config_api_key_valid() -> None:
    auth = AuthConfig(type="api_key", client_id="my-key")
    assert auth.client_id == "my-key"


def test_auth_config_api_key_missing_client_id_raises() -> None:
    with pytest.raises(ValidationError, match="client_id"):
        AuthConfig(type="api_key")


def test_auth_config_bearer_token_valid() -> None:
    auth = AuthConfig(type="bearer_token", bearer_token="tok123")
    assert auth.bearer_token == "tok123"


def test_auth_config_bearer_token_missing_raises() -> None:
    with pytest.raises(ValidationError, match="bearer_token"):
        AuthConfig(type="bearer_token")


# ---------------------------------------------------------------------------
# AuthConfig — basic requires both client_id and client_secret
# ---------------------------------------------------------------------------


def test_auth_config_basic_missing_client_id_raises() -> None:
    with pytest.raises(ValidationError, match="client_id"):
        AuthConfig(type="basic", client_secret="secret")


def test_auth_config_basic_missing_client_secret_raises() -> None:
    with pytest.raises(ValidationError, match="client_secret"):
        AuthConfig(type="basic", client_id="id")


def test_auth_config_basic_valid() -> None:
    auth = AuthConfig(type="basic", client_id="id", client_secret="secret")
    assert auth.client_id == "id"


# ---------------------------------------------------------------------------
# LoadingConfig — disabled skips normalization; s3/azure bucket conflicts
# ---------------------------------------------------------------------------


def test_loading_config_disabled_skips_alias_normalization() -> None:
    cfg = LoadingConfig(enabled=False, destination="local")
    assert cfg.enabled is False


def test_loading_config_s3_conflicting_bucket_raises() -> None:
    with pytest.raises(ValidationError):
        LoadingConfig(destination="s3", bucket="bucket-a", s3_bucket="bucket-b", prefix="a/b")


def test_loading_config_azure_conflicting_bucket_raises() -> None:
    with pytest.raises(ValidationError):
        LoadingConfig(
            destination="azure_blob",
            bucket="container-a",
            azure_container="container-b",
            azure_account="acc",
            prefix="a/b",
        )


# ---------------------------------------------------------------------------
# PreprocessConfig — concat without separator raises
# ---------------------------------------------------------------------------


def test_preprocess_config_concat_missing_separator_raises() -> None:
    with pytest.raises(ValidationError, match="separator"):
        from src.config.config_models import PreprocessConfig, PreprocessorType

        PreprocessConfig(type=PreprocessorType.CONCAT)


# ---------------------------------------------------------------------------
# SnapshotConfig — invalid Python expression in ready_condition raises
# ---------------------------------------------------------------------------


def test_snapshot_config_invalid_expression_raises() -> None:
    from src.config.config_models import SnapshotConfig

    with pytest.raises(ValidationError, match="Invalid Python expression"):
        SnapshotConfig(ready_condition="def bad(:")


# ---------------------------------------------------------------------------
# TableReadOptions — range mode validation
# ---------------------------------------------------------------------------


def test_table_read_options_range_mode_requires_num_partitions() -> None:
    from src.config.config_models import TableReadOptions

    with pytest.raises(ValidationError, match="num_partitions"):
        TableReadOptions(partition_column="id", lower_bound=0, upper_bound=100)


def test_table_read_options_range_mode_requires_bounds() -> None:
    from src.config.config_models import TableReadOptions

    with pytest.raises(ValidationError, match="lower_bound"):
        TableReadOptions(partition_column="id", num_partitions=4)


def test_table_read_options_lower_bound_gt_upper_raises() -> None:
    from src.config.config_models import TableReadOptions

    with pytest.raises(ValidationError, match="lower_bound"):
        TableReadOptions(partition_column="id", num_partitions=4, lower_bound=100, upper_bound=0)


def test_table_read_options_valid_range_mode() -> None:
    from src.config.config_models import TableReadOptions

    opts = TableReadOptions(partition_column="id", num_partitions=4, lower_bound=0, upper_bound=100)
    assert opts.uses_parallel_read() is True


def test_table_read_options_predicates_and_partition_column_raises() -> None:
    from src.config.config_models import TableReadOptions

    with pytest.raises(ValidationError, match="either predicates or partition_column"):
        TableReadOptions(
            partition_column="id",
            num_partitions=4,
            lower_bound=0,
            upper_bound=100,
            predicates=["id > 0"],
        )


# ---------------------------------------------------------------------------
# ResourceConfig — normalize_request_inputs shorthand forms
# ---------------------------------------------------------------------------


def test_resource_config_normalize_request_inputs_scalar_shorthand() -> None:
    res = ResourceConfig(method="GET", path="/r", request_inputs={"k": "static_value"})
    assert "k" in res.request_inputs
    assert res.request_inputs["k"].value == "static_value"


def test_resource_config_normalize_request_inputs_dict_without_value_key() -> None:
    res = ResourceConfig(
        method="GET",
        path="/r",
        request_inputs={"nested": {"a": 1, "b": 2}},
    )
    assert res.request_inputs["nested"].value == {"a": 1, "b": 2}


# ---------------------------------------------------------------------------
# SourceConfig — database source type validation
# ---------------------------------------------------------------------------


def test_source_config_database_requires_host() -> None:
    from src.config.config_models import SourceType

    with pytest.raises(ValidationError, match="host"):
        SourceConfig(
            type=SourceType.POSTGRESQL,
            username="user",
            password="pass",
            port=5432,
            database="db",
            resources={
                "t": ResourceConfig(
                    method="GET",
                    path=None,
                    database_schema="public",
                    database_table="users",
                )
            },
        )
