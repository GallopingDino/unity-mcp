"""Tests for PluginHub ephemeral-mode shutdown."""

import asyncio
import signal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from transport.plugin_hub import (
    PluginHub,
    RECONNECT_GRACE_SECONDS,
    TRANSPORT_RECOVERY_GRACE_SECONDS,
    _ReconnectInfo,
)
from transport.plugin_registry import PluginRegistry
from core.config import config


@pytest.fixture
def plugin_registry():
    return PluginRegistry()


@pytest.fixture
def reset_plugin_hub():
    """Reset PluginHub class state before and after each test."""
    original_registry = PluginHub._registry
    original_lock = PluginHub._lock
    original_loop = PluginHub._loop
    original_deadlines = PluginHub._reconnect_deadlines.copy()
    original_exit_task = PluginHub._exit_task
    original_has_ever = PluginHub._has_ever_connected
    original_cold_start_used = PluginHub._cold_start_grace_used
    original_ephemeral = config.ephemeral_mode
    original_transport = config.transport_mode
    original_remote = config.http_remote_hosted

    yield

    PluginHub._registry = original_registry
    PluginHub._lock = original_lock
    PluginHub._loop = original_loop
    PluginHub._connections.clear()
    PluginHub._pending.clear()
    PluginHub._reconnect_deadlines.clear()
    PluginHub._reconnect_deadlines.update(original_deadlines)
    PluginHub._session_close_causes.clear()
    if PluginHub._exit_task is not None and not PluginHub._exit_task.done():
        PluginHub._exit_task.cancel()
    PluginHub._exit_task = original_exit_task
    PluginHub._has_ever_connected = original_has_ever
    PluginHub._cold_start_grace_used = original_cold_start_used
    config.ephemeral_mode = original_ephemeral
    config.transport_mode = original_transport
    config.http_remote_hosted = original_remote


def _enable_ephemeral(monkeypatch):
    monkeypatch.setattr(config, "transport_mode", "http")
    monkeypatch.setattr(config, "http_remote_hosted", False)
    monkeypatch.setattr(config, "ephemeral_mode", True)


def _configure_hub_in_running_loop(plugin_registry):
    """Configure PluginHub bound to the currently running event loop."""
    loop = asyncio.get_running_loop()
    PluginHub.configure(plugin_registry, loop)


class TestEphemeralActiveGuards:
    """Intent-based shutdown only applies to HTTP local with --ephemeral."""

    def test_inactive_for_stdio(self, monkeypatch, reset_plugin_hub):
        monkeypatch.setattr(config, "transport_mode", "stdio")
        monkeypatch.setattr(config, "http_remote_hosted", False)
        monkeypatch.setattr(config, "ephemeral_mode", True)
        assert PluginHub._ephemeral_active() is False

    def test_inactive_for_remote_hosted(self, monkeypatch, reset_plugin_hub):
        monkeypatch.setattr(config, "transport_mode", "http")
        monkeypatch.setattr(config, "http_remote_hosted", True)
        monkeypatch.setattr(config, "ephemeral_mode", True)
        assert PluginHub._ephemeral_active() is False

    def test_inactive_when_flag_off(self, monkeypatch, reset_plugin_hub):
        monkeypatch.setattr(config, "transport_mode", "http")
        monkeypatch.setattr(config, "http_remote_hosted", False)
        monkeypatch.setattr(config, "ephemeral_mode", False)
        assert PluginHub._ephemeral_active() is False

    def test_active_for_http_local_ephemeral(self, monkeypatch, reset_plugin_hub):
        _enable_ephemeral(monkeypatch)
        assert PluginHub._ephemeral_active() is True


class TestColdStartGrace:
    """Before any plugin has ever registered, the server waits RECONNECT_GRACE_SECONDS."""

    @pytest.mark.asyncio
    async def test_schedules_cold_start_grace(self, monkeypatch, reset_plugin_hub):
        _enable_ephemeral(monkeypatch)
        with patch.object(PluginHub, "_schedule_exit") as scheduled:
            PluginHub._evaluate_exit()
            assert scheduled.call_count == 1
            args, kwargs = scheduled.call_args
            assert args[0] == RECONNECT_GRACE_SECONDS
            assert "cold-start" in (args[1] if len(args) > 1 else kwargs.get("reason", ""))

    @pytest.mark.asyncio
    async def test_cold_start_grace_fires_only_once(self, monkeypatch, reset_plugin_hub):
        """If grace expires without any connection, the second evaluation must exit, not re-arm."""
        _enable_ephemeral(monkeypatch)
        with patch.object(PluginHub, "_schedule_exit") as scheduled, \
             patch("transport.plugin_hub.signal.raise_signal") as raised:
            # First eval: schedules grace.
            PluginHub._evaluate_exit()
            assert scheduled.call_count == 1
            raised.assert_not_called()
            # Second eval (simulating grace timer firing without any connect): exits.
            PluginHub._evaluate_exit()
            assert scheduled.call_count == 1  # not re-armed
            raised.assert_called_once_with(signal.SIGTERM)


class TestExitDecisionAfterDisconnect:
    """When the last connection drops, decide based on reconnect-deadline map."""

    @pytest.mark.asyncio
    async def test_exits_immediately_when_no_deadlines(self, monkeypatch, reset_plugin_hub):
        _enable_ephemeral(monkeypatch)
        PluginHub._has_ever_connected = True
        PluginHub._reconnect_deadlines.clear()
        with patch("transport.plugin_hub.signal.raise_signal") as raised:
            PluginHub._evaluate_exit()
            raised.assert_called_once_with(signal.SIGTERM)

    @pytest.mark.asyncio
    async def test_waits_when_deadlines_alive(self, monkeypatch, reset_plugin_hub):
        _enable_ephemeral(monkeypatch)
        PluginHub._has_ever_connected = True
        import time as _time
        PluginHub._reconnect_deadlines["abc"] = _ReconnectInfo(
            deadline=_time.monotonic() + 100.0, reason="announced"
        )
        with patch.object(PluginHub, "_schedule_exit") as scheduled, \
             patch("transport.plugin_hub.signal.raise_signal") as raised:
            PluginHub._evaluate_exit()
            assert scheduled.call_count == 1
            raised.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_connections_present(self, monkeypatch, reset_plugin_hub):
        _enable_ephemeral(monkeypatch)
        PluginHub._has_ever_connected = True
        PluginHub._connections["sid"] = MagicMock()
        with patch.object(PluginHub, "_schedule_exit") as scheduled, \
             patch("transport.plugin_hub.signal.raise_signal") as raised:
            PluginHub._evaluate_exit()
            scheduled.assert_not_called()
            raised.assert_not_called()


class TestReconnectDeadlineExpiry:
    """Expired reconnect deadlines must not keep the server alive."""

    @pytest.mark.asyncio
    async def test_drops_expired_deadlines(self, monkeypatch, reset_plugin_hub):
        _enable_ephemeral(monkeypatch)
        import time as _time
        PluginHub._reconnect_deadlines["live"] = _ReconnectInfo(
            deadline=_time.monotonic() + 100.0, reason="announced"
        )
        PluginHub._reconnect_deadlines["dead"] = _ReconnectInfo(
            deadline=_time.monotonic() - 1.0, reason="announced"
        )
        PluginHub._drop_expired_deadlines()
        assert "live" in PluginHub._reconnect_deadlines
        assert "dead" not in PluginHub._reconnect_deadlines

    @pytest.mark.asyncio
    async def test_exit_after_expires_all_deadlines(self, monkeypatch, reset_plugin_hub, plugin_registry):
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        PluginHub._has_ever_connected = True
        import time as _time
        PluginHub._reconnect_deadlines["dead"] = _ReconnectInfo(
            deadline=_time.monotonic() - 1.0, reason="announced"
        )
        with patch("transport.plugin_hub.signal.raise_signal") as raised:
            await PluginHub._exit_after(0.0)
            raised.assert_called_once_with(signal.SIGTERM)


class TestExpectReconnectMessage:
    """expect_reconnect marks the instance by project_hash for reconnect."""

    @pytest.mark.asyncio
    async def test_marks_instance_by_project_hash(self, monkeypatch, reset_plugin_hub, plugin_registry):
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        # Pre-register a session so the hub can resolve project_hash from session_id
        session, _ = await plugin_registry.register(
            session_id="sid-1",
            project_name="Proj",
            project_hash="hashA",
            unity_version="2022.3",
        )

        from transport.models import ExpectReconnectMessage
        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())
        await hub._handle_expect_reconnect(
            ExpectReconnectMessage(session_id="sid-1", reason="domain_reload")
        )
        assert "hashA" in PluginHub._reconnect_deadlines
        assert PluginHub._reconnect_deadlines["hashA"].reason == "announced"

    @pytest.mark.asyncio
    async def test_unknown_session_is_ignored(self, monkeypatch, reset_plugin_hub, plugin_registry):
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        from transport.models import ExpectReconnectMessage
        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())
        await hub._handle_expect_reconnect(
            ExpectReconnectMessage(session_id="ghost", reason="domain_reload")
        )
        assert PluginHub._reconnect_deadlines == {}


class TestSessionEndMessage:
    """session_end marks the session as a clean shutdown so on_disconnect skips recovery grace."""

    @pytest.mark.asyncio
    async def test_marks_session_close_cause(self, monkeypatch, reset_plugin_hub, plugin_registry):
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        from transport.models import SessionEndMessage
        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())
        await hub._handle_session_end(
            SessionEndMessage(session_id="sid-quit", reason="editor_quit")
        )
        assert PluginHub._session_close_causes.get("sid-quit") == "session_end"

    @pytest.mark.asyncio
    async def test_session_end_then_disconnect_exits_immediately_no_grace(
        self, monkeypatch, reset_plugin_hub, plugin_registry
    ):
        """The common editor-quit case: client sends session_end, then the
        socket disconnects with close_code=1005 because the close-frame
        doesn't flush before the editor process exits. Must NOT schedule a
        transport-recovery grace; must exit immediately."""
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        PluginHub._has_ever_connected = True

        await plugin_registry.register(
            session_id="sid-quit",
            project_name="Proj",
            project_hash="hash-quit",
            unity_version="2022.3",
        )
        ws = MagicMock()
        PluginHub._connections["sid-quit"] = ws
        PluginHub._session_close_causes["sid-quit"] = "session_end"

        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())

        with patch("transport.plugin_hub.signal.raise_signal") as raised:
            await hub.on_disconnect(ws, 1005)
            raised.assert_called_once_with(signal.SIGTERM)

        assert "hash-quit" not in PluginHub._reconnect_deadlines


class TestRegisterClearsPendingReconnect:
    """A successful register clears the project_hash's reconnect deadline."""

    @pytest.mark.asyncio
    async def test_register_clears_deadline(self, monkeypatch, reset_plugin_hub, plugin_registry):
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        PluginHub._reconnect_deadlines["abc123"] = _ReconnectInfo(
            deadline=1e18, reason="announced"
        )
        ws = AsyncMock()
        ws.headers = {}
        ws.state = SimpleNamespace()
        ws.send_json = AsyncMock()
        ws.close = AsyncMock()
        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())

        await hub.on_receive(ws, {
            "type": "register",
            "project_name": "P",
            "project_hash": "abc123",
            "unity_version": "2022.3",
        })

        assert "abc123" not in PluginHub._reconnect_deadlines
        assert PluginHub._has_ever_connected is True


class TestMultiInstance:
    """Spec section 'Multi-Instance Support'."""

    @pytest.mark.asyncio
    async def test_b_disconnect_while_a_reloading_keeps_alive(self, monkeypatch, reset_plugin_hub):
        _enable_ephemeral(monkeypatch)
        PluginHub._has_ever_connected = True
        # A is reloading, B was connected and just disconnected.
        import time as _time
        PluginHub._reconnect_deadlines["A"] = _ReconnectInfo(
            deadline=_time.monotonic() + 100.0, reason="announced"
        )
        # _connections is empty (B disconnected, A's socket also dropped).
        with patch.object(PluginHub, "_schedule_exit") as scheduled, \
             patch("transport.plugin_hub.signal.raise_signal") as raised:
            PluginHub._evaluate_exit()
            assert scheduled.call_count == 1
            raised.assert_not_called()


class TestLockGuardedExit:
    """on_disconnect must call _evaluate_exit while holding the lock."""

    @pytest.mark.asyncio
    async def test_on_disconnect_evaluates_under_lock(self, monkeypatch, reset_plugin_hub, plugin_registry):
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        PluginHub._has_ever_connected = True
        ws = MagicMock()
        # Inject a fake connected session
        PluginHub._connections["sid-1"] = ws

        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())

        with patch("transport.plugin_hub.signal.raise_signal") as raised:
            await hub.on_disconnect(ws, 1000)
            assert "sid-1" not in PluginHub._connections
            raised.assert_called_once_with(signal.SIGTERM)


class TestTransportRecoveryGrace:
    """Server-initiated stale closes and abnormal disconnects must give the client a
    short grace window to reconnect, instead of treating the disconnect as session-end."""

    @pytest.mark.asyncio
    async def test_stale_ping_disconnect_schedules_recovery_grace(
        self, monkeypatch, reset_plugin_hub, plugin_registry
    ):
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        PluginHub._has_ever_connected = True

        # Pre-register a session in the registry so on_disconnect can resolve project_hash.
        await plugin_registry.register(
            session_id="sid-stale",
            project_name="Proj",
            project_hash="hash-stale",
            unity_version="2022.3",
        )
        ws = MagicMock()
        PluginHub._connections["sid-stale"] = ws
        # Simulate the ping-loop having tagged the session before closing the socket.
        PluginHub._session_close_causes["sid-stale"] = "stale_ping"

        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())

        with patch("transport.plugin_hub.signal.raise_signal") as raised:
            await hub.on_disconnect(ws, 1001)
            raised.assert_not_called()

        info = PluginHub._reconnect_deadlines.get("hash-stale")
        assert info is not None
        assert info.reason == "stale_ping"

    @pytest.mark.asyncio
    async def test_abnormal_disconnect_without_cause_schedules_recovery(
        self, monkeypatch, reset_plugin_hub, plugin_registry
    ):
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        PluginHub._has_ever_connected = True

        await plugin_registry.register(
            session_id="sid-abn",
            project_name="Proj",
            project_hash="hash-abn",
            unity_version="2022.3",
        )
        ws = MagicMock()
        PluginHub._connections["sid-abn"] = ws

        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())

        with patch("transport.plugin_hub.signal.raise_signal") as raised:
            await hub.on_disconnect(ws, 1006)  # abnormal closure
            raised.assert_not_called()

        info = PluginHub._reconnect_deadlines.get("hash-abn")
        assert info is not None
        assert info.reason == "abnormal_disconnect"

    @pytest.mark.asyncio
    async def test_normal_close_exits_immediately_no_grace(
        self, monkeypatch, reset_plugin_hub, plugin_registry
    ):
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        PluginHub._has_ever_connected = True

        await plugin_registry.register(
            session_id="sid-clean",
            project_name="Proj",
            project_hash="hash-clean",
            unity_version="2022.3",
        )
        ws = MagicMock()
        PluginHub._connections["sid-clean"] = ws

        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())

        with patch("transport.plugin_hub.signal.raise_signal") as raised:
            await hub.on_disconnect(ws, 1000)  # normal closure
            raised.assert_called_once_with(signal.SIGTERM)

        assert "hash-clean" not in PluginHub._reconnect_deadlines

    @pytest.mark.parametrize("close_code", [1001, 1005])
    @pytest.mark.asyncio
    async def test_non_normal_close_schedules_recovery_grace(
        self, monkeypatch, reset_plugin_hub, plugin_registry, close_code
    ):
        """Close codes other than 1000 must trigger transport-recovery grace,
        even when the peer apparently sent a close frame.

        In particular, 1005 (NoStatusReceived) covers the case where Unity's
        reload path tries to send expect_reconnect, the send times out, and
        Unity then calls _socket.Abort() — the server sees 1005 with no
        announced reconnect, and the plugin needs the grace window to
        reconnect after the domain reload completes."""
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        PluginHub._has_ever_connected = True

        session_id = f"sid-{close_code}"
        project_hash = f"hash-{close_code}"
        await plugin_registry.register(
            session_id=session_id,
            project_name="Proj",
            project_hash=project_hash,
            unity_version="2022.3",
        )
        ws = MagicMock()
        PluginHub._connections[session_id] = ws

        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())

        with patch("transport.plugin_hub.signal.raise_signal") as raised:
            await hub.on_disconnect(ws, close_code)
            raised.assert_not_called()

        info = PluginHub._reconnect_deadlines.get(project_hash)
        assert info is not None
        assert info.reason == "abnormal_disconnect"

    @pytest.mark.asyncio
    async def test_superseded_session_does_not_schedule_grace(
        self, monkeypatch, reset_plugin_hub, plugin_registry
    ):
        """When a new session for the same project_hash evicts an old one, the
        old session's on_disconnect must not schedule a transport-recovery grace."""
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        PluginHub._has_ever_connected = True

        await plugin_registry.register(
            session_id="sid-old",
            project_name="Proj",
            project_hash="hash-sup",
            unity_version="2022.3",
        )
        old_ws = MagicMock()
        new_ws = MagicMock()
        PluginHub._connections["sid-old"] = old_ws
        PluginHub._connections["sid-new"] = new_ws  # new session already present
        PluginHub._session_close_causes["sid-old"] = "superseded"

        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())

        with patch("transport.plugin_hub.signal.raise_signal") as raised:
            await hub.on_disconnect(old_ws, 1001)
            # New connection still present; no exit, but also no recovery grace recorded.
            raised.assert_not_called()

        assert "hash-sup" not in PluginHub._reconnect_deadlines

    @pytest.mark.asyncio
    async def test_announced_grace_not_downgraded_by_abnormal_close(
        self, monkeypatch, reset_plugin_hub, plugin_registry
    ):
        """If the client already announced a 300s reconnect window, an abnormal
        disconnect afterwards must not shorten it to 60s."""
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        PluginHub._has_ever_connected = True

        await plugin_registry.register(
            session_id="sid-ann",
            project_name="Proj",
            project_hash="hash-ann",
            unity_version="2022.3",
        )
        ws = MagicMock()
        PluginHub._connections["sid-ann"] = ws

        # Pre-set an announced deadline far in the future.
        import time as _time
        far_future = _time.monotonic() + RECONNECT_GRACE_SECONDS
        PluginHub._reconnect_deadlines["hash-ann"] = _ReconnectInfo(
            deadline=far_future, reason="announced"
        )

        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())

        with patch("transport.plugin_hub.signal.raise_signal"):
            await hub.on_disconnect(ws, 1006)

        info = PluginHub._reconnect_deadlines.get("hash-ann")
        assert info is not None
        assert info.reason == "announced"
        assert info.deadline == far_future

    @pytest.mark.asyncio
    async def test_recovery_grace_uses_short_window(
        self, monkeypatch, reset_plugin_hub, plugin_registry
    ):
        """Recovery grace must use the short TRANSPORT_RECOVERY_GRACE_SECONDS, not
        the long announced-reconnect 300s — ephemeral mode must not turn into a
        long-lived background process after a single transient error."""
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)
        PluginHub._has_ever_connected = True

        await plugin_registry.register(
            session_id="sid-w",
            project_name="Proj",
            project_hash="hash-w",
            unity_version="2022.3",
        )
        ws = MagicMock()
        PluginHub._connections["sid-w"] = ws

        import time as _time
        before = _time.monotonic()

        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())
        with patch("transport.plugin_hub.signal.raise_signal"):
            await hub.on_disconnect(ws, 1006)

        after = _time.monotonic()
        info = PluginHub._reconnect_deadlines.get("hash-w")
        assert info is not None
        # Deadline ~ now + TRANSPORT_RECOVERY_GRACE_SECONDS, well below RECONNECT_GRACE_SECONDS.
        assert info.deadline - before <= TRANSPORT_RECOVERY_GRACE_SECONDS + 1.0
        assert info.deadline - after >= TRANSPORT_RECOVERY_GRACE_SECONDS - 1.0
        assert TRANSPORT_RECOVERY_GRACE_SECONDS < RECONNECT_GRACE_SECONDS


class TestWelcomeFlags:
    """Welcome message must surface ephemeral / httpRemoteHosted flags from config."""

    @pytest.mark.asyncio
    async def test_welcome_includes_ephemeral_and_remote_hosted(
        self, monkeypatch, reset_plugin_hub, plugin_registry
    ):
        _enable_ephemeral(monkeypatch)
        _configure_hub_in_running_loop(plugin_registry)

        ws = AsyncMock()
        ws.headers = {}
        ws.state = SimpleNamespace()
        ws.accept = AsyncMock()
        ws.send_json = AsyncMock()

        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())
        await hub.on_connect(ws)

        ws.send_json.assert_called_once()
        payload = ws.send_json.call_args.args[0]
        assert payload["type"] == "welcome"
        assert payload["ephemeral"] is True
        assert payload["httpRemoteHosted"] is False
        assert payload["serverTimeout"] == PluginHub.SERVER_TIMEOUT
        assert payload["keepAliveInterval"] == PluginHub.KEEP_ALIVE_INTERVAL

    @pytest.mark.asyncio
    async def test_welcome_reflects_remote_hosted_flag(
        self, monkeypatch, reset_plugin_hub, plugin_registry
    ):
        monkeypatch.setattr(config, "transport_mode", "http")
        monkeypatch.setattr(config, "http_remote_hosted", False)
        monkeypatch.setattr(config, "ephemeral_mode", False)
        _configure_hub_in_running_loop(plugin_registry)

        ws = AsyncMock()
        ws.headers = {}
        ws.state = SimpleNamespace()
        ws.accept = AsyncMock()
        ws.send_json = AsyncMock()

        hub = PluginHub({"type": "websocket"}, receive=AsyncMock(), send=AsyncMock())
        await hub.on_connect(ws)

        payload = ws.send_json.call_args.args[0]
        assert payload["ephemeral"] is False
        assert payload["httpRemoteHosted"] is False


class TestEphemeralCli:
    """`--ephemeral` CLI flag wiring."""

    @pytest.fixture
    def stub_mcp_server(self):
        with patch("main.create_mcp_server") as factory:
            factory.return_value = MagicMock()
            yield factory

    def test_flag_sets_ephemeral_mode(self, monkeypatch, stub_mcp_server):
        import sys as _sys
        monkeypatch.setattr(config, "ephemeral_mode", False)
        monkeypatch.setattr(_sys, "argv", ["main", "--ephemeral"])

        from main import main
        main()

        assert config.ephemeral_mode is True

    def test_flag_ignored_with_remote_hosted(self, monkeypatch, stub_mcp_server):
        import sys as _sys
        import logging
        monkeypatch.setenv("UNITY_MCP_API_KEY_VALIDATION_URL", "http://example.test/validate")
        monkeypatch.setattr(config, "ephemeral_mode", False)
        monkeypatch.setattr(_sys, "argv", [
            "main", "--ephemeral", "--http-remote-hosted", "--transport", "http",
        ])

        # Attach a capturing handler to the named logger (propagate=False at module level
        # blocks caplog from receiving these records).
        captured: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured.append(record)

        named_logger = logging.getLogger("mcp-for-unity-server")
        handler = _Capture(level=logging.WARNING)
        named_logger.addHandler(handler)
        try:
            from main import main
            main()
        finally:
            named_logger.removeHandler(handler)

        assert config.ephemeral_mode is False
        assert any("--ephemeral is ignored" in r.getMessage() for r in captured)
