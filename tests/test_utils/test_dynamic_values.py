"""Tests for dynamic value resolution, Jinja helpers, and request-body resolution."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import ValidationError

from src.utils.dynamic_values import (
    ComplexDynamicValue,
    DatabricksDeltaTableConfig,
    DateConfig,
    DateOperation,
    DynamicValueResolver,
    DynamicValueType,
    EnvProxy,
    FilterConfig,
    FilterOperator,
    FilterType,
    FilterValueSource,
    JinjaValueResolver,
    ValueResolver,
    get_resolver,
    resolve_headers_dict,
    resolve_request_body,
)
from src.utils.exceptions import ResolverError


def test_filter_config_rejects_nondefault_operator_for_non_column_type() -> None:
    with pytest.raises(ValidationError, match="operator can only be specified"):
        FilterConfig(
            type=FilterType.PARAMS,
            field="x",
            operator=FilterOperator.NOT_EQUALS,
            value_source=FilterValueSource(source="p", field="id"),
        )


def test_filter_config_params_properties() -> None:
    fc = FilterConfig(
        type=FilterType.PARAMS,
        field="_params",
        value_source=FilterValueSource(source="parent", field="pid"),
    )
    assert fc.is_params_filter is True
    assert fc.params_key == "parent__pid"


def test_filter_config_params_key_without_value_source() -> None:
    fc = FilterConfig(type=FilterType.PARAMS, field="f", value_source="static")
    assert fc.params_key is None


@pytest.fixture
def fixed_resolver(monkeypatch: pytest.MonkeyPatch) -> DynamicValueResolver:
    redis = MagicMock()
    r = DynamicValueResolver(redis_context=redis)
    # Midnight so MONTH_END (first of next month minus 1s) lands on the prior calendar day.
    fixed = datetime(2024, 6, 15, 0, 0, 0, tzinfo=UTC)
    r._now = fixed
    r._now_ts = int(fixed.timestamp())
    r._now_ms = int(fixed.timestamp() * 1000)
    return r


def test_get_timestamp_variants(fixed_resolver: DynamicValueResolver) -> None:
    assert fixed_resolver.get_timestamp(DynamicValueType.NOW_UNIX) == str(fixed_resolver._now_ts)
    assert "2024-06-15" in fixed_resolver.get_timestamp(DynamicValueType.NOW_ISO)
    assert fixed_resolver.get_timestamp(DynamicValueType.NOW_MS) == str(fixed_resolver._now_ms)
    with pytest.raises(ValueError, match="Not a timestamp"):
        fixed_resolver.get_timestamp(DynamicValueType.UUID)  # type: ignore[arg-type]


def test_get_date_operations(fixed_resolver: DynamicValueResolver) -> None:
    assert fixed_resolver.get_date(DateConfig(operation=DateOperation.TODAY)) == "2024-06-15"
    assert (
        fixed_resolver.get_date(DateConfig(operation=DateOperation.DAYS_AGO, days=5))
        == "2024-06-10"
    )
    assert (
        fixed_resolver.get_date(DateConfig(operation=DateOperation.DAYS_FUTURE, days=3))
        == "2024-06-18"
    )
    linkedin = fixed_resolver.get_date(
        DateConfig(operation=DateOperation.LINKEDIN_PREVIOUS_MONTH_RANGE)
    )
    assert "start:" in linkedin and "end:" in linkedin
    assert fixed_resolver.get_date(DateConfig(operation=DateOperation.MONTH_START)) == "2024-06-01"
    assert fixed_resolver.get_date(DateConfig(operation=DateOperation.MONTH_END)) == "2024-06-30"
    assert (
        fixed_resolver.get_date(DateConfig(operation=DateOperation.PREVIOUS_MONTH_START))
        == "2024-05-01"
    )
    assert (
        fixed_resolver.get_date(DateConfig(operation=DateOperation.PREVIOUS_MONTH_END))
        == "2024-05-31"
    )
    # June 15 2024 is Saturday; implementation walks week boundaries from weekday().
    assert (
        fixed_resolver.get_date(DateConfig(operation=DateOperation.PREVIOUS_SUNDAY)) == "2024-06-02"
    )
    assert (
        fixed_resolver.get_date(DateConfig(operation=DateOperation.PREVIOUS_SATURDAY))
        == "2024-06-08"
    )


def test_get_date_invalid_operation(fixed_resolver: DynamicValueResolver) -> None:
    bad = DateConfig.model_construct(operation="not_an_enum_member")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="Unsupported date operation"):
        fixed_resolver.get_date(bad)


def test_resolve_databricks_requires_query_ref(fixed_resolver: DynamicValueResolver) -> None:
    with pytest.raises(ValueError, match="query_ref"):
        fixed_resolver.resolve_databricks_delta_table_value(
            DatabricksDeltaTableConfig(query_ref="")
        )


def test_resolve_databricks_redis_hit(fixed_resolver: DynamicValueResolver) -> None:
    cfg = DatabricksDeltaTableConfig(query_ref="my.query")
    fixed_resolver.redis_context.get.return_value = [{"id": 1}]
    assert fixed_resolver.resolve_databricks_delta_table_value(cfg) == [{"id": 1}]


def test_resolve_databricks_requires_redis_manager_present() -> None:
    """Without any Redis manager, resolution cannot read cached query results."""
    cfg = DatabricksDeltaTableConfig(query_ref="q")
    r = DynamicValueResolver(redis_context=None)  # type: ignore[arg-type]
    with pytest.raises(ResolverError, match="Redis Context is required"):
        r.resolve_databricks_delta_table_value(cfg)


def test_compute_rsa_signature_roundtrip(fixed_resolver: DynamicValueResolver) -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    sig = fixed_resolver.compute_rsa_signature(["a", "b"], pem, "SHA256")
    assert isinstance(sig, str) and len(sig) > 8


def test_compute_rsa_signature_invalid_key(fixed_resolver: DynamicValueResolver) -> None:
    with pytest.raises(ValueError, match="Failed to compute RSA"):
        fixed_resolver.compute_rsa_signature(["x"], "not-a-key", "SHA256")


def test_resolve_legacy_static_and_pagination() -> None:
    redis = MagicMock()
    r = DynamicValueResolver(redis_context=redis)
    assert r.resolve(42) == 42
    pag = ComplexDynamicValue(type=DynamicValueType.PAGINATION, pagination_config={})
    assert r.resolve(pag) == 1


def test_resolve_flat_date_dict(fixed_resolver: DynamicValueResolver) -> None:
    out = fixed_resolver.resolve(
        {
            "type": "DATE",
            "operation": DateOperation.TODAY.value,
            "days": 0,
            "format": "%Y-%m-%d",
        }
    )
    assert out == "2024-06-15"


def test_jinja_value_resolver_requires_context() -> None:
    with pytest.raises(ValueError, match="Either redis_context"):
        JinjaValueResolver(redis_context=None, legacy_resolver=None)  # type: ignore[arg-type]


def test_jinja_resolve_non_template_passthrough() -> None:
    redis = MagicMock()
    jr = JinjaValueResolver(redis_context=redis)
    assert jr.resolve("plain") == "plain"


def test_jinja_resolve_single_expression_raw_type() -> None:
    redis = MagicMock()
    jr = JinjaValueResolver(redis_context=redis)
    assert jr.resolve("{{ now_unix() }}").isdigit()


def test_jinja_resolve_mixed_template() -> None:
    redis = MagicMock()
    jr = JinjaValueResolver(redis_context=redis)
    out = jr.resolve("x-{{ now_unix() }}-y")
    assert out.startswith("x-") and out.endswith("-y")


def test_env_proxy_access() -> None:
    import os

    os.environ["TEST_ENV_PROXY_K"] = "v"
    try:
        p = EnvProxy()
        assert p.TEST_ENV_PROXY_K == "v"
        assert p["TEST_ENV_PROXY_K"] == "v"
        assert p.get("TEST_ENV_PROXY_K") == "v"
        assert p.get("MISSING", "d") == "d"
    finally:
        os.environ.pop("TEST_ENV_PROXY_K", None)


def test_value_resolver_nested_value_key() -> None:
    redis = MagicMock()
    vr = ValueResolver(redis_context=redis)
    assert vr.resolve({"value": "{{ now_unix() }}", "meta": 1}) == vr.resolve(
        "{{ now_unix() }}", context=None
    )


def test_resolve_headers_dict_requires_one_of() -> None:
    with pytest.raises(ValueError, match="Either redis_context"):
        resolve_headers_dict({"a": 1}, redis_context=None, resolver=None)


def test_resolve_headers_dict_with_resolver() -> None:
    redis = MagicMock()
    vr = get_resolver(redis)
    out = resolve_headers_dict({"h": "{{ now_unix() }}"}, resolver=vr)
    assert str(out["h"]).isdigit()


def test_resolve_request_body_empty_raw_with_overrides() -> None:
    redis = MagicMock()
    vr = get_resolver(redis)
    body = resolve_request_body({}, vr, overrides={"k": "{{ now_unix() }}"})
    assert str(body["k"]).isdigit()


def test_resolve_request_body_two_pass_jinja() -> None:
    redis = MagicMock()
    vr = get_resolver(redis)
    raw = {"prefix": "x-", "joined": "{{ prefix }}{{ now_unix() }}"}
    out = resolve_request_body(raw, vr)
    assert out["prefix"] == "x-"
    assert out["joined"].startswith("x-") and out["joined"][2:].isdigit()


def test_resolve_request_body_exclude_keys() -> None:
    redis = MagicMock()
    vr = get_resolver(redis)
    out = resolve_request_body({"x": 1, "y": 2}, vr, exclude_keys=["x"])
    assert "x" not in out and out["y"] == 2


def test_jinja_single_expression_falls_back_to_render(monkeypatch: pytest.MonkeyPatch) -> None:
    jr = JinjaValueResolver(redis_context=MagicMock())

    class _Expr:
        def __call__(self, **_ctx):
            raise RuntimeError("compile path failed")

    class _Template:
        def render(self, **_ctx):
            return "render-ok"

    monkeypatch.setattr(jr._env, "compile_expression", lambda _inner: _Expr())
    monkeypatch.setattr(jr._env, "from_string", lambda _tpl: _Template())
    assert jr.resolve("{{ now_unix() }}") == "render-ok"


def test_resolve_headers_dict_mixed_values_uses_redis_context_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = MagicMock()
    resolver.resolve.side_effect = lambda v: f"resolved:{v}"
    monkeypatch.setattr("src.utils.dynamic_values.get_resolver", lambda _rc: resolver)
    out = resolve_headers_dict({"A": "v", "B": 2}, redis_context=MagicMock())
    assert out == {"A": "resolved:v", "B": "resolved:2"}
    assert resolver.resolve.call_count == 2


def test_resolve_request_body_overrides_precedence_with_nested_values() -> None:
    vr = MagicMock()
    vr.resolve.side_effect = lambda v, context=None: (
        f"{context['prefix']}-suffix"
        if isinstance(v, str) and v == "{{ prefix }}-suffix"
        else {"resolved": True} if isinstance(v, dict) else [1, 2, 3] if isinstance(v, list) else v
    )
    out = resolve_request_body(
        {"prefix": "raw", "joined": "{{ prefix }}-suffix", "obj": {"x": 1}, "arr": [1]},
        vr,
        overrides={"prefix": "override"},
        exclude_keys=["arr"],
    )
    assert out["prefix"] == "override"
    assert out["joined"] == "override-suffix"
    assert out["obj"] == {"resolved": True}
    assert "arr" not in out


def test_resolve_request_body_bubbles_resolver_errors() -> None:
    vr = MagicMock()
    vr.resolve.side_effect = ResolverError("bad body")
    with pytest.raises(ResolverError, match="bad body"):
        resolve_request_body({"x": {"k": "v"}}, vr)
