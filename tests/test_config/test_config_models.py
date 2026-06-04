"""Tests for config model helpers."""

import json
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from src.config.config_models import (
    AuthConfig,
    ContextConfig,
    ContextType,
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
    SnapshotConfig,
    SourceConfig,
    SourceType,
    SparkRuntimeConfig,
    StreamingConfig,
    TableReadOptions,
    is_database_source_type,
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


def test_get_effective_loading_destinations_skips_disabled_source_without_selection() -> None:
    """Full-config semantics: disabled sources must not pull Spark object-store packages."""
    cfg = PipelineConfig(
        config_root=Path("."),
        version="1.0",
        defaults=DefaultsConfig(),
        queries=[],
        sources={
            "disabled_only": SourceConfig(
                enabled=False,
                type="rest_api",
                base_url="https://example.com",
                resources={
                    "posts": ResourceConfig(
                        enabled=True,
                        path="/posts",
                        method="GET",
                        loading={"destination": "s3", "s3_bucket": "bucket-x", "prefix": "a/b"},
                    ),
                },
            ),
        },
    )
    assert cfg.get_effective_loading_destinations() == set()


def test_get_effective_loading_destinations_includes_disabled_source_when_cli_selected() -> None:
    """CLI ``--select`` forces ExecutionPlan inclusion even when ``enabled: false`` on the source."""
    cfg = PipelineConfig(
        config_root=Path("."),
        version="1.0",
        runtime_selection={"disabled_only": None},
        defaults=DefaultsConfig(),
        queries=[],
        sources={
            "disabled_only": SourceConfig(
                enabled=False,
                type="rest_api",
                base_url="https://example.com",
                resources={
                    "posts": ResourceConfig(
                        enabled=True,
                        path="/posts",
                        method="GET",
                        loading={"destination": "s3", "s3_bucket": "bucket-x", "prefix": "a/b"},
                    ),
                },
            ),
        },
    )
    assert cfg.get_effective_loading_destinations() == {"s3"}


def test_get_effective_loading_destinations_whole_source_selection_respects_disabled_resources() -> (
    None
):
    """``-s source`` (all resources): disabled tables are excluded from the plan and from Spark deps."""
    cfg = PipelineConfig(
        config_root=Path("."),
        version="1.0",
        runtime_selection={"api": None},
        defaults=DefaultsConfig(),
        queries=[],
        sources={
            "api": SourceConfig(
                enabled=True,
                type="rest_api",
                base_url="https://example.com",
                resources={
                    "on": ResourceConfig(
                        enabled=True,
                        path="/on",
                        method="GET",
                        loading={"destination": "gcs", "gcs_bucket": "bucket-a", "prefix": "a/b"},
                    ),
                    "off": ResourceConfig(
                        enabled=False,
                        path="/off",
                        method="GET",
                        loading={"destination": "s3", "s3_bucket": "bucket-x", "prefix": "a/b"},
                    ),
                },
            ),
        },
    )
    assert cfg.get_effective_loading_destinations() == {"gcs"}


def test_get_effective_loading_destinations_includes_disabled_resource_when_explicitly_selected() -> (
    None
):
    """``-s source:resource``: disabled resource is still executed by the planner; Spark must see its destination."""
    cfg = PipelineConfig(
        config_root=Path("."),
        version="1.0",
        runtime_selection={"api": {"off"}},
        defaults=DefaultsConfig(),
        queries=[],
        sources={
            "api": SourceConfig(
                enabled=True,
                type="rest_api",
                base_url="https://example.com",
                resources={
                    "on": ResourceConfig(
                        enabled=True,
                        path="/on",
                        method="GET",
                        loading={"destination": "gcs", "gcs_bucket": "bucket-a", "prefix": "a/b"},
                    ),
                    "off": ResourceConfig(
                        enabled=False,
                        path="/off",
                        method="GET",
                        loading={"destination": "s3", "s3_bucket": "bucket-x", "prefix": "a/b"},
                    ),
                },
            ),
        },
    )
    assert cfg.get_effective_loading_destinations() == {"s3"}


# ---------------------------------------------------------------------------
# InputConfig.format_request_value
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fmt_type, input_value, expected",
    [
        (RequestFormatType.STRING, "hello", "hello"),
        (RequestFormatType.STRING, ["a", "b"], "a"),  # list → first element as str
        (RequestFormatType.INTEGER, "3", 3),
        (RequestFormatType.FLOAT, "1.5", 1.5),
        (RequestFormatType.ARRAY, "x", ["x"]),  # scalar → wrapped in list
        (RequestFormatType.ARRAY, [1, 2], [1, 2]),  # list → passthrough
        (RequestFormatType.JSON_STRING, [1, 2], json.dumps([1, 2])),
        (RequestFormatType.JSON_STRING, {"k": "v"}, json.dumps({"k": "v"})),
    ],
)
def test_input_config_format_request_value_parametrized(fmt_type, input_value, expected) -> None:
    cfg = InputConfig(value="placeholder", request_format=RequestFormatConfig(type=fmt_type))
    assert cfg.format_request_value(input_value) == expected


def test_input_config_format_request_value_boolean_and_list_wrapped_numeric() -> None:
    """Branches shared with parametrized matrix; kept explicit for scalar list handling."""
    cfg_b = InputConfig(
        value="placeholder", request_format=RequestFormatConfig(type=RequestFormatType.BOOLEAN)
    )
    assert cfg_b.format_request_value(1) is True
    assert cfg_b.format_request_value(0) is False

    cfg_i = InputConfig(
        value="placeholder", request_format=RequestFormatConfig(type=RequestFormatType.INTEGER)
    )
    assert cfg_i.format_request_value(["5"]) == 5

    cfg_f = InputConfig(
        value="placeholder", request_format=RequestFormatConfig(type=RequestFormatType.FLOAT)
    )
    assert cfg_f.format_request_value(["2.0"]) == 2.0


def test_input_config_format_request_value_no_format_passthrough() -> None:
    cfg = InputConfig(value="x")
    assert cfg.format_request_value("anything") == "anything"
    assert cfg.format_request_value(None) is None


# ---------------------------------------------------------------------------
# InputConfig preprocessing — CONCAT
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "separator, input_value, expected",
    [
        (",", ["a", "b", "c"], "a,b,c"),
        ("-", "x", "x"),  # scalar wrapped to single-element list before join
    ],
)
def test_input_config_preprocess_concat(separator, input_value, expected) -> None:
    cfg = InputConfig(
        value="placeholder",
        request_format=RequestFormatConfig(
            type=RequestFormatType.STRING,
            preprocess=[PreprocessConfig(type=PreprocessorType.CONCAT, separator=separator)],
        ),
    )
    assert cfg.format_request_value(input_value) == expected


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


@pytest.mark.parametrize(
    "merge_keys",
    [pytest.param(None, id="missing"), pytest.param([], id="empty_list")],
)
def test_loading_config_merge_mode_requires_nonempty_merge_keys(merge_keys) -> None:
    kwargs: dict = {
        "destination": "local",
        "storage_root": "/tmp",
        "write_mode": "merge",
    }
    if merge_keys is not None:
        kwargs["merge_keys"] = merge_keys
    with pytest.raises(ValidationError, match="merge_keys"):
        LoadingConfig(**kwargs)


# ---------------------------------------------------------------------------
# LoadingConfig — output_partitions validation
# ---------------------------------------------------------------------------


def test_loading_config_output_partitions_defaults_to_none() -> None:
    cfg = LoadingConfig(destination="local", storage_root="/tmp")
    assert cfg.output_partitions is None


def test_loading_config_output_partitions_accepts_valid_value() -> None:
    cfg = LoadingConfig(destination="local", storage_root="/tmp", output_partitions=8)
    assert cfg.output_partitions == 8


def test_loading_config_output_partitions_rejects_zero() -> None:
    with pytest.raises(ValidationError, match="output_partitions"):
        LoadingConfig(destination="local", storage_root="/tmp", output_partitions=0)


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


# ---------------------------------------------------------------------------
# is_database_source_type
# ---------------------------------------------------------------------------


def test_is_database_source_type_matches_relational_kinds() -> None:
    assert is_database_source_type(SourceType.POSTGRESQL) is True
    assert is_database_source_type(SourceType.HANA) is True
    assert is_database_source_type(SourceType.REST_API) is False
    assert is_database_source_type(SourceType.PYTHON_SDK) is False


# ---------------------------------------------------------------------------
# ContextConfig — Redis required when type is redis
# ---------------------------------------------------------------------------


def test_context_config_redis_type_requires_redis_block() -> None:
    with pytest.raises(ValidationError, match="Redis configuration required"):
        ContextConfig(type=ContextType.REDIS, redis=None)


def test_context_config_redis_with_block_validates() -> None:
    from src.config.config_models import RedisConfig

    cfg = ContextConfig(type=ContextType.REDIS, redis=RedisConfig(host="h"))
    assert cfg.redis is not None


# ---------------------------------------------------------------------------
# LoadingConfig — prefix shape for object-store destinations
# ---------------------------------------------------------------------------


def test_loading_config_prefix_single_segment_raises() -> None:
    with pytest.raises(ValidationError, match="prefix must follow"):
        LoadingConfig(destination="s3", s3_bucket="b", prefix="only_segment")


def test_loading_config_prefix_must_not_include_data_segment() -> None:
    with pytest.raises(ValidationError, match="prefix should not include"):
        LoadingConfig(destination="s3", s3_bucket="b", prefix="src/res/data")


# ---------------------------------------------------------------------------
# ResourceConfig.resolve_parameters
# ---------------------------------------------------------------------------


def test_resource_config_resolve_parameters_formats_inputs_and_passes_extras() -> None:
    redis_context = MagicMock()
    res = ResourceConfig(
        method="GET",
        path="/r",
        request_inputs={
            "q": RequestInputConfig(
                value="unused",
                location="query",
                request_format=RequestFormatConfig(type=RequestFormatType.STRING),
            ),
        },
    )
    out = res.resolve_parameters(
        redis_context,
        params={"q": ["a", "b"], "extra_plain": 1},
    )
    assert out["q"] == "a"
    assert out["extra_plain"] == 1


def test_resource_config_resolve_parameters_custom_param_dict_non_input_config() -> None:
    redis_context = MagicMock()
    res = ResourceConfig(method="GET", path="/r")
    out = res.resolve_parameters(
        redis_context,
        params={"x": 1},
        param_dict={"plain_key": "literal"},
    )
    assert out == {"plain_key": "literal"}


def test_resource_config_resolve_parameters_resolves_dynamic_value(monkeypatch) -> None:
    redis_context = MagicMock()
    resolver = MagicMock()
    resolver.resolve.return_value = "dyn-resolved"

    monkeypatch.setattr(
        "src.config.config_models.get_resolver",
        lambda _rc: resolver,
    )
    res = ResourceConfig(
        method="GET",
        path="/r",
        request_inputs={
            "p": RequestInputConfig(value="{{dyn}}", location="query"),
        },
    )
    out = res.resolve_parameters(redis_context, params={})
    assert resolver.resolve.call_count == 1
    assert out["p"] == "dyn-resolved"


# ---------------------------------------------------------------------------
# InputConfig — helpers and format_request_value edge cases
# ---------------------------------------------------------------------------


def test_input_config_format_request_value_none_returns_none() -> None:
    cfg = InputConfig(value="x", request_format=RequestFormatConfig(type=RequestFormatType.STRING))
    assert cfg.format_request_value(None) is None


def test_input_config_get_source_and_filter_config() -> None:
    dyn = ComplexDynamicValue(
        type=DynamicValueType.SOURCE,
        source_config=DynamicSourceReference(source="p", field="id"),
    )
    cfg = InputConfig(value=dyn)
    assert cfg.get_source_config() is dyn.source_config
    assert cfg.get_filter_config() is None

    dyn_f = ComplexDynamicValue(
        type=DynamicValueType.SOURCE,
        source_config=DynamicSourceReference(
            source="p",
            field="id",
            filter=FilterConfig(field="s", value_source="active"),
        ),
    )
    cfg_f = InputConfig(value=dyn_f)
    assert cfg_f.get_filter_config() is dyn_f.source_config.filter


def test_input_config_get_databricks_query_refs() -> None:
    cfg = InputConfig(value="prefix databricks( 'my_query_ref' ) suffix")
    assert cfg.get_databricks_query_refs() == ["my_query_ref"]


def test_input_config_preprocess_unsupported_concat_without_separator_raises() -> None:
    bad_step = PreprocessConfig.model_construct(type=PreprocessorType.CONCAT, separator=None)
    rf = RequestFormatConfig.model_construct(
        type=RequestFormatType.STRING,
        preprocess=[bad_step],
    )
    cfg = InputConfig.model_construct(value="x", request_format=rf, input_format="single")
    with pytest.raises(ValueError, match="Unsupported preprocessing type"):
        cfg.format_request_value(["a", "b"])


# ---------------------------------------------------------------------------
# TableReadOptions — empty predicates list
# ---------------------------------------------------------------------------


def test_table_read_options_empty_predicates_raises() -> None:
    with pytest.raises(ValidationError, match="predicates must be non-empty"):
        TableReadOptions(predicates=[])


# ---------------------------------------------------------------------------
# SourceConfig — snapshot only for rest_api
# ---------------------------------------------------------------------------


def test_source_config_snapshot_on_non_rest_api_raises() -> None:
    with pytest.raises(ValidationError, match="snapshot polling"):
        SourceConfig(
            type=SourceType.PYTHON_SDK,
            sdk=PythonSDKConfig(module="mod", class_name="Cls"),
            resources={
                "r": ResourceConfig(
                    method="run",
                    path="/r",
                    snapshot=SnapshotConfig(ready_condition="True"),
                )
            },
        )


def test_spark_runtime_event_log_requires_dir() -> None:
    with pytest.raises(ValidationError, match="spark_event_log_dir"):
        SparkRuntimeConfig(spark_event_log_enabled=True)


def test_spark_runtime_event_log_accepts_dir() -> None:
    c = SparkRuntimeConfig(spark_event_log_enabled=True, spark_event_log_dir="/tmp/spark-events")
    assert c.spark_event_log_enabled is True
    assert c.spark_event_log_dir == "/tmp/spark-events"
