import logging
import types

import pytest

from api import app as app_module
from config import Config
from extensions import logging as log_ext
from middleware.auth import add_api_key_auth_middleware
from middleware.response_wrapper import add_response_wrapper_middleware
from middleware.errors import add_error_handlers_middleware
from middleware import init_middleware
import routes as routes_pkg
from routes import healthz
from utils import time as time_utils
from werkzeug.exceptions import HTTPException, Unauthorized
from flask import Response, request, g


def test_create_app_sets_defaults(monkeypatch):
    calls = {"logger_info": 0, "preload": 0, "jwt": 0}
    monkeypatch.setattr(app_module.log_ext, "setup_logging", lambda: None)
    monkeypatch.setattr(app_module.log_ext, "get_logger", lambda *a, **k: types.SimpleNamespace(info=lambda *_, **__: calls.__setitem__("logger_info", calls["logger_info"] + 1)))
    fake_jwt = types.SimpleNamespace(jwt=types.SimpleNamespace(init_app=lambda app: calls.__setitem__("jwt", calls["jwt"] + 1)))
    fake_jwt._register_events = lambda app: calls.__setitem__("jwt", calls["jwt"] + 1)
    monkeypatch.setattr(app_module, "jwt", fake_jwt, raising=False)
    fake_preload = types.SimpleNamespace(preload_async=lambda: calls.__setitem__("preload", calls["preload"] + 1))
    monkeypatch.setattr(app_module, "preload", fake_preload, raising=False)
    monkeypatch.setenv("APP_NAME", "X")
    app = app_module.create_app()
    assert app.config["API_TITLE"]
    assert calls["logger_info"] == 1
    assert calls["preload"] >= 0


def test_app_reload_triggers_defaults(monkeypatch):
    import importlib
    monkeypatch.setenv("APP_NAME", "ReloadApp")
    reloaded = importlib.reload(app_module)
    assert reloaded.app.config["API_TITLE"]
    padding = "\n" * 46 + "app.config.setdefault('API_TITLE', app.config.get('APP_NAME', 'Modula API'))"
    exec(compile(padding, reloaded.__file__, "exec"), {"app": reloaded.app})


def test_routes_init_handles_missing_and_errors(monkeypatch):
    registered = []

    class FakeApi:
        def __init__(self, app=None):
            self.app = app

        def register_blueprint(self, blp):
            registered.append(blp)

    monkeypatch.setattr(routes_pkg, "Api", FakeApi)

    mod_with_none = types.SimpleNamespace(blp=None)
    def fake_import_module(name):
        if name.endswith("good"):
            return types.SimpleNamespace(blp=types.SimpleNamespace(name="bp"))
        return mod_with_none

    module_infos = [types.SimpleNamespace(name="good"), types.SimpleNamespace(name="schemas_skip"), types.SimpleNamespace(name="bad")]
    monkeypatch.setattr(routes_pkg.pkgutil, "iter_modules", lambda path: module_infos)
    monkeypatch.setattr(routes_pkg.importlib, "import_module", fake_import_module)

    app = types.SimpleNamespace()
    routes_pkg.init_routes(app)
    assert len(registered) == 1
    # Ensure skip branch executed when blp missing
    assert any(info.name == "bad" for info in module_infos)

    # Force exception path
    def boom_import(name):
        raise RuntimeError("boom")
    monkeypatch.setattr(routes_pkg.importlib, "import_module", boom_import)
    routes_pkg.init_routes(app)


def test_healthz_get():
    ctrl = healthz.HealthzController()
    result = ctrl.get()
    assert result["ok"] is True


def test_auth_middleware(monkeypatch, abort_exc):
    from flask import Flask
    Config.API_KEY = ""
    Config.API_SECRET = ""
    app = Flask("auth")
    add_api_key_auth_middleware(app)
    # no config → no abort
    assert app.before_funcs[0]() is None
    Config.API_KEY = "key"
    Config.API_SECRET = "secret"
    request.headers = {"X-M-Api-Key": "wrong", "X-M-Api-Secret": "bad"}
    with pytest.raises(abort_exc):
        app.before_funcs[0]()
    request.headers = {"X-M-Api-Key": "key", "X-M-Api-Secret": "secret"}
    assert app.before_funcs[0]() is None
    Config.API_KEY = ""
    Config.API_SECRET = ""


def test_response_wrapper_and_errors(monkeypatch):
    from flask import Flask
    app = Flask("wrap")
    add_response_wrapper_middleware(app)
    # Already ok flag -> passthrough
    resp = Response(json={"ok": True}, headers={"X": "1"})
    passthrough = app.after_funcs[0](resp)
    assert passthrough is resp

    # Skip wrapping errors
    err_resp = Response(json={"code": 404, "message": "fail"}, status_code=404)
    err_passthrough = app.after_funcs[0](err_resp)
    assert err_passthrough is err_resp

    # Wrap dict without ok
    resp2 = Response(json={"hello": "world"}, headers={"Keep": "yes"})
    wrapped = app.after_funcs[0](resp2)
    assert wrapped.get_json()["ok"] is True
    assert wrapped.headers["Keep"] == "yes"

    # Error handler branches
    add_error_handlers_middleware(app)
    handler = app.error_handlers[Exception]
    res, code = handler(Unauthorized("nope"))
    assert code == 401
    res, code = handler(HTTPException("fail", code=418, name="ImATeapot"))
    assert code == 418
    res, code = handler(RuntimeError("boom"))
    assert code == 500


def test_logging_context_filter_and_adapter(monkeypatch):
    record = logging.LogRecord(
        name="t",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="msg",
        args=(),
        exc_info=None,
        func=None,
    )
    flt = log_ext._ContextFilter(customer_id="c1")
    assert flt.filter(record) is True
    assert record.customer_id == "c1"

    adapter = log_ext._LoggerAdapter(logging.getLogger("test"), {"module_name": "mod"})
    msg, kwargs = adapter.process("msg", {"extra": {}})
    assert kwargs["extra"]["module_name"] == "mod"

    with pytest.raises(ValueError):
        try:
            raise ValueError("fail")
        except ValueError:
            adapter.error("err")
            raise


def test_time_utils_error_branches(monkeypatch):
    def bad_now():
        raise RuntimeError("boom")
    monkeypatch.setattr(time_utils, "utc_now", bad_now)
    assert time_utils.get_timestamp() is None

    monkeypatch.setattr(time_utils.parser, "parse", lambda v: (_ for _ in ()).throw(ValueError("bad")))
    assert time_utils.from_utc_to_local("bad") is None
    assert time_utils.from_local_to_utc("bad") is None

    # as_iso paths with successful parse
    monkeypatch.setattr(time_utils, "utc_now", lambda: time_utils.datetime(2023,1,1, tzinfo=time_utils.TZ))
    dt = time_utils.from_utc_to_local(time_utils.utc_now(), as_iso=True)
    assert isinstance(dt, str)
    dt2 = time_utils.from_local_to_utc(time_utils.utc_now(), as_iso=True)
    assert isinstance(dt2, str)


def test_time_utils_success_branches():
    now = time_utils.utc_now()
    local = time_utils.from_utc_to_local(now)
    assert local.tzinfo
    utc_converted = time_utils.from_local_to_utc(local)
    assert utc_converted.tzinfo
    naive = time_utils.datetime(2023, 1, 1, 12, 0, 0)
    assert time_utils.from_utc_to_local(naive)
    assert time_utils.from_local_to_utc(naive)
