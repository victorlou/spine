"""Tests for src.get_token helper branches."""

from types import SimpleNamespace

import pytest

import src.get_token as get_token


def _settings_for(source_name: str, source_config):
    return SimpleNamespace(
        pipeline_config=SimpleNamespace(
            defaults=SimpleNamespace(
                context=SimpleNamespace(
                    type="redis",
                    ttl=30,
                    prefix="p:",
                    redis=SimpleNamespace(model_dump=lambda: {"host": "x"}),
                )
            ),
            sources={source_name: source_config},
        )
    )


def test_get_redis_context_and_walmart_style_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    src = SimpleNamespace(headers={"WM_SEC.AUTH_SIGNATURE": "x", "WM_CONSUMER.INTIMESTAMP": "y"})
    assert get_token._is_walmart_style(src) is True
    assert get_token._is_walmart_style(SimpleNamespace(headers={})) is False

    called = {}

    class _FakeRedis:
        def __init__(self, redis_config, prefix, default_ttl):
            called["args"] = (redis_config, prefix, default_ttl)

    monkeypatch.setattr(get_token, "RedisContextManager", _FakeRedis)
    ctx = get_token._get_redis_context(_settings_for("s", src))
    assert ctx is not None
    assert called["args"][1] == "p:"


def test_main_missing_source_arg_exits(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(get_token, "load_pipeline_dotenv", lambda: None)
    monkeypatch.setattr(get_token, "process_environment_variables", lambda: None)
    monkeypatch.setattr(get_token.sys, "argv", ["prog"])
    with pytest.raises(SystemExit) as exc:
        get_token.main()
    assert exc.value.code == 1
    assert "Please provide a source name" in capsys.readouterr().out


def test_main_non_token_auth_exits_zero(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    src_cfg = SimpleNamespace(base_url="https://x", headers={}, auth=SimpleNamespace(type="basic"))
    settings = _settings_for("s", src_cfg)
    monkeypatch.setattr(get_token, "load_pipeline_dotenv", lambda: None)
    monkeypatch.setattr(get_token, "process_environment_variables", lambda: None)
    monkeypatch.setattr(get_token.sys, "argv", ["prog", "s"])
    monkeypatch.setattr(get_token, "get_settings", lambda selection=None: settings)
    monkeypatch.setattr(get_token, "_get_redis_context", lambda _s: object())
    with pytest.raises(SystemExit) as exc:
        get_token.main()
    assert exc.value.code == 0
    assert "does not use token authentication" in capsys.readouterr().out
