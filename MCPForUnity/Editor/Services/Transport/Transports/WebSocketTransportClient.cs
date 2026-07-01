using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MCPForUnity.Editor.Constants;
using MCPForUnity.Editor.Helpers;
using MCPForUnity.Editor.Services;
using MCPForUnity.Editor.Services.Transport;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using UnityEditor;
using UnityEngine;

namespace MCPForUnity.Editor.Services.Transport.Transports
{
    /// <summary>
    /// Maintains a persistent WebSocket connection to the MCP server plugin hub.
    /// Handles registration, keep-alives, and command dispatch back into Unity via
    /// <see cref="TransportCommandDispatcher"/>.
    /// </summary>
    public class WebSocketTransportClient : IMcpTransportClient, IDisposable
    {
        private const string TransportDisplayName = "websocket";
        private static readonly TimeSpan[] ReconnectSchedule =
        {
            TimeSpan.Zero,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(3),
            TimeSpan.FromSeconds(5),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(30)
        };
        private static readonly TimeSpan ReconnectTailInterval = TimeSpan.FromSeconds(30);

        private static readonly TimeSpan DefaultKeepAliveInterval = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan DefaultCommandTimeout = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(30);
        // Threshold beyond which a single send is treated as suspicious and bumped to WARN.
        private const long SlowSendWarnMs = 500;
        // Threshold beyond which the synchronous main-thread tool list fetch is treated as suspicious.
        private const long SlowMainThreadWaitWarnMs = 1000;
        // Multiplier applied to keep-alive interval to detect a stalled pong cadence.
        private const int PongStalenessMultiplier = 2;

        private readonly IToolDiscoveryService _toolDiscoveryService;
        private ClientWebSocket _socket;
        private CancellationTokenSource _lifecycleCts;
        private CancellationTokenSource _connectionCts;
        private Task _receiveTask;
        private Task _keepAliveTask;
        private Task _heartbeatTask;
        private readonly SemaphoreSlim _sendLock = new(1, 1);

        private Uri _endpointUri;
        private string _sessionId;
        private string _projectHash;
        private string _projectName;
        private string _projectPath;
        private string _unityVersion;
        private TimeSpan _keepAliveInterval = DefaultKeepAliveInterval;
        private TimeSpan _socketKeepAliveInterval = DefaultKeepAliveInterval;
        private volatile bool _isConnected;
        private int _isReconnectingFlag;
        private TransportState _state = TransportState.Disconnected(TransportDisplayName, "Transport not started");
        private string _apiKey;
        private bool _disposed;
        private volatile bool _serverEphemeral;
        private volatile bool _serverHttpRemoteHosted;
        private volatile bool _welcomeReceived;

        // TickCount64 timestamps for liveness tracking. Read via Interlocked.Read so 32-bit hosts
        // see a torn-free value. 0 means "never observed yet".
        private long _lastPingRecvTicks;
        private long _lastPongSentTicks;

        public WebSocketTransportClient(IToolDiscoveryService toolDiscoveryService = null)
        {
            _toolDiscoveryService = toolDiscoveryService;
        }

        public bool IsConnected => _isConnected;
        public string TransportName => TransportDisplayName;
        public TransportState State => _state;

        /// <summary>Server self-reported ephemeral lifecycle (true → simplified UI).</summary>
        public bool ServerEphemeral => _serverEphemeral;

        /// <summary>Server self-reported as remote-hosted (true → simplified UI).</summary>
        public bool ServerHttpRemoteHosted => _serverHttpRemoteHosted;

        /// <summary>True after the first welcome message has been processed.</summary>
        public bool WelcomeReceived => _welcomeReceived;

        /// <summary>
        /// True when the underlying socket is open and the server has assigned a session id —
        /// i.e. <see cref="TrySendExpectReconnectSync"/> can succeed right now. This becomes
        /// true on the receive thread as soon as the <c>registered</c> message is processed,
        /// independent of <see cref="TransportManager"/>'s aggregated state which is only
        /// updated when the <see cref="StartAsync"/> continuation resumes on the captured
        /// SynchronizationContext. Reload hooks that need to know whether a usable session
        /// exists must consult this rather than the aggregated state to avoid a window where
        /// the socket is registered but the manager has not yet observed it.
        /// </summary>
        public bool HasLiveSession =>
            _socket != null
            && _socket.State == WebSocketState.Open
            && !string.IsNullOrEmpty(_sessionId);

        private Task<List<ToolMetadata>> GetEnabledToolsOnMainThreadAsync(CancellationToken token)
        {
            return TransportCommandDispatcher.RunOnMainThreadAsync(
                () => _toolDiscoveryService?.GetEnabledTools() ?? new List<ToolMetadata>(),
                token);
        }

        public async Task<bool> StartAsync()
        {
            // Capture identity values on the main thread before any async context switching
            _projectName = ProjectIdentityUtility.GetProjectName();
            _projectHash = ProjectIdentityUtility.GetProjectHash();
            _unityVersion = Application.unityVersion;
            _apiKey = HttpEndpointUtility.IsRemoteScope()
                ? EditorPrefs.GetString(EditorPrefKeys.ApiKey, string.Empty)
                : string.Empty;

            if (HttpEndpointUtility.IsRemoteScope()
                && !HttpEndpointUtility.IsCurrentRemoteUrlAllowed(out string remoteUrlError))
            {
                string message = remoteUrlError ?? "HTTP Remote URL is not allowed by current security settings.";
                _state = TransportState.Disconnected(TransportDisplayName, message);
                McpLog.Error($"[WebSocket] {message}");
                return false;
            }

            // Get project root path (strip /Assets from dataPath) for focus nudging
            string dataPath = Application.dataPath;
            if (!string.IsNullOrEmpty(dataPath))
            {
                string normalized = dataPath.TrimEnd('/', '\\');
                if (string.Equals(System.IO.Path.GetFileName(normalized), "Assets", StringComparison.Ordinal))
                {
                    _projectPath = System.IO.Path.GetDirectoryName(normalized) ?? normalized;
                }
                else
                {
                    _projectPath = normalized;  // Fallback if path doesn't end with Assets
                }
            }

            await StopAsync();

            _lifecycleCts = new CancellationTokenSource();
            _endpointUri = BuildWebSocketUri(HttpEndpointUtility.GetBaseUrl());
            _sessionId = null;

            McpDiagnosticLog.Info("WS", $"StartAsync project={_projectName} hash={_projectHash} unity={_unityVersion} endpoint={_endpointUri}");

            if (!await EstablishConnectionAsync(_lifecycleCts.Token))
            {
                McpDiagnosticLog.Warn("WS", "StartAsync: EstablishConnectionAsync returned false");
                await StopAsync();
                return false;
            }

            // State is connected but session ID might be pending until 'registered' message
            _state = TransportState.Connected(TransportDisplayName, sessionId: "pending", details: _endpointUri.ToString());
            _isConnected = true;
            McpDiagnosticLog.Info("WS", $"StartAsync ok endpoint={_endpointUri}");
            return true;
        }

        public async Task StopAsync()
        {
            if (_lifecycleCts == null)
            {
                return;
            }

            McpDiagnosticLog.Info("WS", $"StopAsync session={_sessionId ?? "(none)"} socketState={_socket?.State.ToString() ?? "null"}");

            try
            {
                _lifecycleCts.Cancel();
            }
            catch { }

            await StopConnectionLoopsAsync().ConfigureAwait(false);

            if (_socket != null)
            {
                try
                {
                    if (_socket.State == WebSocketState.Open || _socket.State == WebSocketState.CloseReceived)
                    {
                        // Send our close frame (code 1000) without waiting for the server's
                        // acknowledgement. CloseAsync waits for the full bidirectional
                        // handshake, which frequently fails to complete within the Editor's
                        // 750ms shutdown budget — especially under --ephemeral, where the
                        // server starts its own shutdown the moment it processes our close
                        // frame and may not get a chance to send its close-back. The result
                        // was that the server saw a truncated/missing payload (close code
                        // 1005) instead of a clean 1000, and could not distinguish editor
                        // quit from a transient abort. CloseOutputAsync just enqueues our
                        // close frame; the OS TCP stack flushes it before Dispose closes
                        // the connection, so the server reliably receives code 1000.
                        using var sendCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
                        try
                        {
                            await _socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "Shutdown", sendCts.Token).ConfigureAwait(false);
                            McpDiagnosticLog.Info("WS", "StopAsync: close-output frame sent");
                        }
                        catch (OperationCanceledException)
                        {
                            McpDiagnosticLog.Warn("WS", "StopAsync: close-output send timed out after 200ms — server may see abnormal disconnect");
                        }
                    }
                }
                catch (Exception ex) { McpDiagnosticLog.Exception("WS", "StopAsync: close-output failed", ex); }
                finally
                {
                    _socket.Dispose();
                    _socket = null;
                }
            }

            _isConnected = false;
            _state = TransportState.Disconnected(TransportDisplayName);
            _welcomeReceived = false;
            _serverEphemeral = false;
            _serverHttpRemoteHosted = false;

            _lifecycleCts.Dispose();
            _lifecycleCts = null;
        }

        /// <summary>
        /// Synchronous teardown for use in beforeAssemblyReload where async is not possible.
        /// Skips the graceful WebSocket close handshake and just disposes resources immediately.
        /// The server handles ungraceful disconnects via its ping timeout.
        /// </summary>
        public void ForceStop()
        {
            McpDiagnosticLog.Info("WS", $"ForceStop session={_sessionId ?? "(none)"} socketState={_socket?.State.ToString() ?? "null"}");

            try { _lifecycleCts?.Cancel(); } catch { }
            try { _connectionCts?.Cancel(); } catch { }

            if (_socket != null)
            {
                try { _socket.Abort(); } catch (Exception ex) { McpDiagnosticLog.Exception("WS", "ForceStop: socket Abort failed", ex); }
                try { _socket.Dispose(); } catch { }
                _socket = null;
            }

            try { _connectionCts?.Dispose(); } catch { }
            _connectionCts = null;
            _receiveTask = null;
            _keepAliveTask = null;
            _heartbeatTask = null;
            Interlocked.Exchange(ref _isReconnectingFlag, 0);
            _isConnected = false;
            _state = TransportState.Disconnected(TransportDisplayName);
            _welcomeReceived = false;
            _serverEphemeral = false;
            _serverHttpRemoteHosted = false;

            try { _lifecycleCts?.Dispose(); } catch { }
            _lifecycleCts = null;
        }

        /// <summary>
        /// Best-effort synchronous send used in <c>beforeAssemblyReload</c>, where
        /// async is not honored. Waits up to <paramref name="timeoutMs"/>ms for
        /// the bytes to leave the socket; if the send times out, the caller
        /// proceeds with <see cref="ForceStop"/> and the server will treat the
        /// disconnect as a session end.
        /// </summary>
        public bool TrySendExpectReconnectSync(string reason, int timeoutMs = 200)
        {
            if (_socket == null || _socket.State != WebSocketState.Open)
            {
                McpDiagnosticLog.Warn("WS", $"expect_reconnect skipped: socketState={_socket?.State.ToString() ?? "null"} reason={reason}");
                return false;
            }
            string sessionId = _sessionId;
            if (string.IsNullOrEmpty(sessionId))
            {
                McpDiagnosticLog.Warn("WS", $"expect_reconnect skipped: sessionId not yet assigned (welcome not processed) reason={reason}");
                return false;
            }

            var payload = new JObject
            {
                ["type"] = "expect_reconnect",
                ["reason"] = reason,
                ["session_id"] = sessionId
            };
            byte[] bytes = Encoding.UTF8.GetBytes(payload.ToString(Formatting.None));
            var buffer = new ArraySegment<byte>(bytes);

            McpDiagnosticLog.Info("Send", $"expect_reconnect reason={reason} session={sessionId} timeoutMs={timeoutMs}");

            try
            {
                Task sendTask = _socket.SendAsync(buffer, WebSocketMessageType.Text, true, CancellationToken.None);
                bool ok = sendTask.Wait(TimeSpan.FromMilliseconds(timeoutMs));
                if (ok)
                {
                    McpDiagnosticLog.Info("WS", $"expect_reconnect send completed reason={reason}");
                }
                else
                {
                    McpDiagnosticLog.Warn("WS", $"expect_reconnect send TIMED OUT after {timeoutMs}ms reason={reason}; ForceStop will likely abort the socket before the byte leaves TCP — server will treat disconnect as session end");
                }
                return ok;
            }
            catch (Exception ex)
            {
                McpDiagnosticLog.Exception("WS", $"expect_reconnect send threw reason={reason}", ex);
                return false;
            }
        }

        /// <summary>
        /// Best-effort synchronous send of a <c>session_end</c> message used in
        /// <c>EditorApplication.quitting</c>, where async is not honored. Mirrors
        /// <see cref="TrySendExpectReconnectSync"/> in shape: waits up to
        /// <paramref name="timeoutMs"/>ms for the bytes to leave the socket so
        /// the server can mark the session as cleanly ending and skip the
        /// transport-recovery grace it would otherwise apply to a 1005 close.
        /// </summary>
        public bool TrySendSessionEndSync(string reason, int timeoutMs = 200)
        {
            if (_socket == null || _socket.State != WebSocketState.Open)
            {
                McpDiagnosticLog.Warn("WS", $"session_end skipped: socketState={_socket?.State.ToString() ?? "null"} reason={reason}");
                return false;
            }
            string sessionId = _sessionId;
            if (string.IsNullOrEmpty(sessionId))
            {
                McpDiagnosticLog.Warn("WS", $"session_end skipped: sessionId not yet assigned (welcome not processed) reason={reason}");
                return false;
            }

            var payload = new JObject
            {
                ["type"] = "session_end",
                ["reason"] = reason,
                ["session_id"] = sessionId
            };
            byte[] bytes = Encoding.UTF8.GetBytes(payload.ToString(Formatting.None));
            var buffer = new ArraySegment<byte>(bytes);

            McpDiagnosticLog.Info("Send", $"session_end reason={reason} session={sessionId} timeoutMs={timeoutMs}");

            try
            {
                Task sendTask = _socket.SendAsync(buffer, WebSocketMessageType.Text, true, CancellationToken.None);
                bool ok = sendTask.Wait(TimeSpan.FromMilliseconds(timeoutMs));
                if (ok)
                {
                    McpDiagnosticLog.Info("WS", $"session_end send completed reason={reason}");
                }
                else
                {
                    McpDiagnosticLog.Warn("WS", $"session_end send TIMED OUT after {timeoutMs}ms reason={reason}; server may still apply transport-recovery grace");
                }
                return ok;
            }
            catch (Exception ex)
            {
                McpDiagnosticLog.Exception("WS", $"session_end send threw reason={reason}", ex);
                return false;
            }
        }

        public async Task<bool> VerifyAsync()
        {
            if (_socket == null || _socket.State != WebSocketState.Open)
            {
                return false;
            }

            if (_lifecycleCts == null)
            {
                return false;
            }

            try
            {
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(_lifecycleCts.Token);
                timeoutCts.CancelAfter(TimeSpan.FromSeconds(5));
                await SendPongAsync(timeoutCts.Token, "verify").ConfigureAwait(false);
                return true;
            }
            catch (Exception ex)
            {
                McpLog.Warn($"[WebSocket] Verify ping failed: {ex.Message}");
                return false;
            }
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                // Ensure background loops are stopped before disposing shared resources
                StopAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                McpLog.Warn($"[WebSocket] Dispose failed to stop cleanly: {ex.Message}");
            }

            _sendLock?.Dispose();
            _socket?.Dispose();
            _lifecycleCts?.Dispose();
            _disposed = true;
        }

        private async Task<bool> EstablishConnectionAsync(CancellationToken token)
        {
            await StopConnectionLoopsAsync().ConfigureAwait(false);

            _connectionCts?.Dispose();
            _connectionCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            CancellationToken connectionToken = _connectionCts.Token;

            Uri originalEndpoint = _endpointUri;
            Uri connectedEndpoint = null;
            Exception lastConnectError = null;

            foreach (Uri candidate in BuildConnectionCandidateUris(originalEndpoint))
            {
                connectionToken.ThrowIfCancellationRequested();

                _socket?.Dispose();
                _socket = new ClientWebSocket();
                _socket.Options.KeepAliveInterval = _socketKeepAliveInterval;

                // Add API key header if configured (for remote-hosted mode)
                if (!string.IsNullOrEmpty(_apiKey))
                {
                    _socket.Options.SetRequestHeader(AuthConstants.ApiKeyHeader, _apiKey);
                }

                McpDiagnosticLog.Info("WS", $"connect attempt host={candidate.Host} port={candidate.Port} scheme={candidate.Scheme}");

                try
                {
                    await _socket.ConnectAsync(candidate, connectionToken).ConfigureAwait(false);
                    connectedEndpoint = candidate;
                    McpDiagnosticLog.Info("WS", $"connect ok host={candidate.Host} port={candidate.Port}");
                    break;
                }
                catch (OperationCanceledException) when (connectionToken.IsCancellationRequested)
                {
                    McpDiagnosticLog.Info("WS", $"connect cancelled host={candidate.Host}");
                    throw;
                }
                catch (Exception ex)
                {
                    lastConnectError = ex;
                    McpLog.Debug($"[WebSocket] Connect failed for {candidate}: {ex.Message}");
                    McpDiagnosticLog.Warn("WS", $"connect failed host={candidate.Host} port={candidate.Port} err={ex.GetType().Name}: {ex.Message}");
                }
            }

            if (connectedEndpoint == null)
            {
                string errorMsg = "Connection failed. Check that the server URL is correct, the server is running, and your API key (if required) is valid.";
                McpLog.Error($"[WebSocket] {errorMsg} (Detail: {lastConnectError?.Message ?? "Unknown error"})");
                McpDiagnosticLog.Error("WS", $"all connect candidates failed; last error: {lastConnectError?.GetType().Name}: {lastConnectError?.Message ?? "unknown"}");
                _state = TransportState.Disconnected(TransportDisplayName, errorMsg);
                return false;
            }

            if (!string.Equals(connectedEndpoint.Host, originalEndpoint.Host, StringComparison.OrdinalIgnoreCase))
            {
                McpLog.Warn($"[WebSocket] Connected via fallback host '{connectedEndpoint.Host}' after '{originalEndpoint.Host}' failed.");
                _endpointUri = connectedEndpoint;
            }

            StartBackgroundLoops(connectionToken);

            try
            {
                await SendRegisterAsync(connectionToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                string regMsg = $"Registration with server failed: {ex.Message}";
                McpLog.Error($"[WebSocket] {regMsg}");
                McpDiagnosticLog.Exception("WS", "register send failed", ex);
                _state = TransportState.Disconnected(TransportDisplayName, regMsg);
                return false;
            }

            return true;
        }

        /// <summary>
        /// Stops the connection loops and disposes of the connection CTS.
        /// Particularly useful when reconnecting, we want to ensure that background loops are cancelled correctly before starting new oens
        /// </summary>
        /// <param name="awaitTasks">Whether to await the receive and keep alive tasks before disposing.</param>
        private async Task StopConnectionLoopsAsync(bool awaitTasks = true)
        {
            if (_connectionCts != null && !_connectionCts.IsCancellationRequested)
            {
                try { _connectionCts.Cancel(); } catch { }
            }

            if (_receiveTask != null)
            {
                if (awaitTasks)
                {
                    try { await _receiveTask.ConfigureAwait(false); } catch { }
                    _receiveTask = null;
                }
                else if (_receiveTask.IsCompleted)
                {
                    _receiveTask = null;
                }
            }

            if (_keepAliveTask != null)
            {
                if (awaitTasks)
                {
                    try { await _keepAliveTask.ConfigureAwait(false); } catch { }
                    _keepAliveTask = null;
                }
                else if (_keepAliveTask.IsCompleted)
                {
                    _keepAliveTask = null;
                }
            }

            if (_heartbeatTask != null)
            {
                if (awaitTasks)
                {
                    try { await _heartbeatTask.ConfigureAwait(false); } catch { }
                    _heartbeatTask = null;
                }
                else if (_heartbeatTask.IsCompleted)
                {
                    _heartbeatTask = null;
                }
            }

            if (_connectionCts != null)
            {
                _connectionCts.Dispose();
                _connectionCts = null;
            }
        }

        private void StartBackgroundLoops(CancellationToken token)
        {
            if ((_receiveTask != null && !_receiveTask.IsCompleted) || (_keepAliveTask != null && !_keepAliveTask.IsCompleted))
            {
                return;
            }

            _receiveTask = Task.Run(() => ReceiveLoopAsync(token), CancellationToken.None);
            _keepAliveTask = Task.Run(() => KeepAliveLoopAsync(token), CancellationToken.None);
            _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(token), CancellationToken.None);
        }

        private async Task ReceiveLoopAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    string message = await ReceiveMessageAsync(token).ConfigureAwait(false);
                    if (message == null)
                    {
                        continue;
                    }
                    await HandleMessageAsync(message, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (WebSocketException wse)
                {
                    McpLog.Warn($"[WebSocket] Receive loop error: {wse.Message}");
                    McpDiagnosticLog.Exception("WS", $"receive loop WebSocketException code={wse.WebSocketErrorCode}", wse);
                    await HandleSocketClosureAsync(wse.Message).ConfigureAwait(false);
                    break;
                }
                catch (Exception ex)
                {
                    McpLog.Warn($"[WebSocket] Unexpected receive error: {ex.Message}");
                    McpDiagnosticLog.Exception("WS", "receive loop unexpected error", ex);
                    await HandleSocketClosureAsync(ex.Message).ConfigureAwait(false);
                    break;
                }
            }
        }

        private async Task<string> ReceiveMessageAsync(CancellationToken token)
        {
            if (_socket == null)
            {
                return null;
            }

            byte[] rentedBuffer = System.Buffers.ArrayPool<byte>.Shared.Rent(8192);
            var buffer = new ArraySegment<byte>(rentedBuffer);
            using var ms = new MemoryStream(8192);

            try
            {
                while (!token.IsCancellationRequested)
                {
                    WebSocketReceiveResult result = await _socket.ReceiveAsync(buffer, token).ConfigureAwait(false);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        McpDiagnosticLog.Info("WS", $"server initiated close code={result.CloseStatus} desc='{result.CloseStatusDescription}'");
                        await HandleSocketClosureAsync(result.CloseStatusDescription ?? "Server closed connection").ConfigureAwait(false);
                        return null;
                    }

                    if (result.Count > 0)
                    {
                        ms.Write(buffer.Array!, buffer.Offset, result.Count);
                    }

                    if (result.EndOfMessage)
                    {
                        break;
                    }
                }

                if (ms.Length == 0)
                {
                    return null;
                }

                return Encoding.UTF8.GetString(ms.ToArray());
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(rentedBuffer);
            }
        }

        private async Task HandleMessageAsync(string message, CancellationToken token)
        {
            JObject payload;
            try
            {
                payload = JObject.Parse(message);
            }
            catch (Exception ex)
            {
                McpLog.Warn($"[WebSocket] Invalid JSON payload: {ex.Message}");
                return;
            }

            string messageType = payload.Value<string>("type") ?? string.Empty;

            string recvSummary = messageType;
            if (messageType == "execute")
            {
                recvSummary = $"execute id={payload.Value<string>("id")} name={payload.Value<string>("name")} timeout={payload.Value<int?>("timeout")}";
            }
            McpDiagnosticLog.Trace("Recv", recvSummary);

            switch (messageType)
            {
                case "welcome":
                    ApplyWelcome(payload);
                    break;
                case "registered":
                    await HandleRegisteredAsync(payload, token).ConfigureAwait(false);
                    break;
                case "execute":
                    await HandleExecuteAsync(payload, token).ConfigureAwait(false);
                    break;
                case "ping":
                    Interlocked.Exchange(ref _lastPingRecvTicks, McpDiagnosticHooks.MonotonicMs());
                    await SendPongAsync(token, "ping_reply").ConfigureAwait(false);
                    break;
                default:
                    // No-op for unrecognised types (keep-alives, telemetry, etc.)
                    break;
            }
        }

        private void ApplyWelcome(JObject payload)
        {
            int? keepAliveSeconds = payload.Value<int?>("keepAliveInterval");
            if (keepAliveSeconds.HasValue && keepAliveSeconds.Value > 0)
            {
                _keepAliveInterval = TimeSpan.FromSeconds(keepAliveSeconds.Value);
                _socketKeepAliveInterval = _keepAliveInterval;
            }

            int? serverTimeoutSeconds = payload.Value<int?>("serverTimeout");
            if (serverTimeoutSeconds.HasValue)
            {
                int sourceSeconds = keepAliveSeconds ?? serverTimeoutSeconds.Value;
                int safeSeconds = Math.Max(5, Math.Min(serverTimeoutSeconds.Value, sourceSeconds));
                _socketKeepAliveInterval = TimeSpan.FromSeconds(safeSeconds);
            }

            _serverEphemeral = payload.Value<bool?>("ephemeral") ?? false;
            _serverHttpRemoteHosted = payload.Value<bool?>("httpRemoteHosted") ?? false;
            _welcomeReceived = true;

            McpDiagnosticLog.Info("WS", $"welcome ephemeral={_serverEphemeral} httpRemoteHosted={_serverHttpRemoteHosted} keepAlive={_keepAliveInterval.TotalSeconds}s socketKeepAlive={_socketKeepAliveInterval.TotalSeconds}s");
        }

        private async Task HandleRegisteredAsync(JObject payload, CancellationToken token)
        {
            string newSessionId = payload.Value<string>("session_id");
            if (!string.IsNullOrEmpty(newSessionId))
            {
                _sessionId = newSessionId;
                ProjectIdentityUtility.SetSessionId(_sessionId);
                _state = TransportState.Connected(TransportDisplayName, sessionId: _sessionId, details: _endpointUri.ToString());
                McpLog.Info($"[WebSocket] Registered with session ID: {_sessionId}", false);
                McpDiagnosticLog.Info("WS", $"registered session={_sessionId}");

                try
                {
                    await SendRegisterToolsAsync(token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    McpDiagnosticLog.Warn("WS", $"register_tools cancelled before send completed session={_sessionId} (likely domain reload mid-handshake — server will not receive tool list)");
                    throw;
                }
                catch (Exception ex)
                {
                    McpDiagnosticLog.Exception("WS", $"register_tools failed session={_sessionId}", ex);
                    throw;
                }
            }
            else
            {
                McpDiagnosticLog.Warn("WS", "registered payload missing session_id");
            }
        }

        private async Task SendRegisterToolsAsync(CancellationToken token)
        {
            if (_toolDiscoveryService == null) return;

            token.ThrowIfCancellationRequested();
            McpDiagnosticLog.Trace("WS", $"register_tools: requesting tool list from main thread session={_sessionId}");
            var mainThreadSw = System.Diagnostics.Stopwatch.StartNew();
            var tools = await GetEnabledToolsOnMainThreadAsync(token).ConfigureAwait(false);
            mainThreadSw.Stop();
            if (mainThreadSw.ElapsedMilliseconds >= SlowMainThreadWaitWarnMs)
            {
                McpDiagnosticLog.Warn(
                    "WS",
                    $"register_tools: main-thread tool fetch took {mainThreadSw.ElapsedMilliseconds}ms (slow) {McpDiagnosticHooks.GetHealthSnapshot()}");
            }
            else
            {
                McpDiagnosticLog.Trace("WS", $"register_tools: main-thread tool fetch took {mainThreadSw.ElapsedMilliseconds}ms");
            }
            token.ThrowIfCancellationRequested();
            McpLog.Info($"[WebSocket] Preparing to register {tools.Count} tool(s) with the bridge.", false);
            McpDiagnosticLog.Info("WS", $"register_tools: building payload tools={tools.Count} session={_sessionId}");
            var toolsArray = new JArray();

            foreach (var tool in tools)
            {
                var toolObj = new JObject
                {
                    ["name"] = tool.Name,
                    ["description"] = tool.Description,
                    ["structured_output"] = tool.StructuredOutput,
                    ["requires_polling"] = tool.RequiresPolling,
                    ["poll_action"] = tool.PollAction ?? "status",
                    ["max_poll_seconds"] = tool.MaxPollSeconds,
                    ["group"] = string.IsNullOrWhiteSpace(tool.Group) ? "core" : tool.Group
                };

                var paramsArray = new JArray();
                if (tool.Parameters != null)
                {
                    foreach (var p in tool.Parameters)
                    {
                        paramsArray.Add(new JObject
                        {
                            ["name"] = p.Name,
                            ["description"] = p.Description,
                            ["type"] = p.Type,
                            ["required"] = p.Required,
                            ["default_value"] = p.DefaultValue
                        });
                    }
                }
                toolObj["parameters"] = paramsArray;
                toolsArray.Add(toolObj);
            }

            var payload = new JObject
            {
                ["type"] = "register_tools",
                ["tools"] = toolsArray
            };

            await SendJsonAsync(payload, token).ConfigureAwait(false);
            McpLog.Info($"[WebSocket] Sent {tools.Count} tools registration", false);
            McpDiagnosticLog.Info("WS", $"register_tools sent tools={tools.Count} session={_sessionId}");
        }

        public async Task ReregisterToolsAsync()
        {
            if (!IsConnected || _lifecycleCts == null)
            {
                McpLog.Warn("[WebSocket] Cannot reregister tools: not connected");
                return;
            }

            try
            {
                await SendRegisterToolsAsync(_lifecycleCts.Token).ConfigureAwait(false);
                McpLog.Info("[WebSocket] Tool reregistration completed", false);
            }
            catch (System.OperationCanceledException)
            {
                McpLog.Warn("[WebSocket] Tool reregistration cancelled");
            }
            catch (System.Exception ex)
            {
                McpLog.Error($"[WebSocket] Tool reregistration failed: {ex.Message}");
            }
        }

        private async Task HandleExecuteAsync(JObject payload, CancellationToken token)
        {
            string commandId = payload.Value<string>("id");
            string commandName = payload.Value<string>("name");
            JObject parameters = payload.Value<JObject>("params") ?? new JObject();
            int timeoutSeconds = payload.Value<int?>("timeout") ?? (int)DefaultCommandTimeout.TotalSeconds;

            if (string.IsNullOrEmpty(commandId) || string.IsNullOrEmpty(commandName))
            {
                McpLog.Warn("[WebSocket] Invalid execute payload (missing id or name)");
                return;
            }

            var commandEnvelope = new JObject
            {
                ["type"] = commandName,
                ["params"] = parameters
            };

            string responseJson;
            var execStopwatch = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                timeoutCts.CancelAfter(TimeSpan.FromSeconds(Math.Max(1, timeoutSeconds)));
                responseJson = await TransportCommandDispatcher.ExecuteCommandJsonAsync(commandEnvelope.ToString(Formatting.None), timeoutCts.Token).ConfigureAwait(false);
                execStopwatch.Stop();
                McpDiagnosticLog.Trace("Exec", $"id={commandId} name={commandName} ok ms={execStopwatch.ElapsedMilliseconds}");
            }
            catch (OperationCanceledException)
            {
                execStopwatch.Stop();
                McpDiagnosticLog.Warn("Exec", $"id={commandId} name={commandName} TIMEOUT after {timeoutSeconds}s (ms={execStopwatch.ElapsedMilliseconds})");
                responseJson = JsonConvert.SerializeObject(new
                {
                    status = "error",
                    error = $"Command '{commandName}' timed out after {timeoutSeconds} seconds"
                });
            }
            catch (Exception ex)
            {
                execStopwatch.Stop();
                McpDiagnosticLog.Exception("Exec", $"id={commandId} name={commandName} failed ms={execStopwatch.ElapsedMilliseconds}", ex);
                responseJson = JsonConvert.SerializeObject(new
                {
                    status = "error",
                    error = ex.Message
                });
            }

            JToken resultToken;
            try
            {
                resultToken = JToken.Parse(responseJson);
            }
            catch
            {
                resultToken = new JObject
                {
                    ["status"] = "error",
                    ["error"] = "Invalid response payload"
                };
            }

            var responsePayload = new JObject
            {
                ["type"] = "command_result",
                ["id"] = commandId,
                ["result"] = resultToken
            };

            McpDiagnosticLog.Trace("Send", $"command_result id={commandId} bytes={responsePayload.ToString(Formatting.None).Length}");
            await SendJsonAsync(responsePayload, token).ConfigureAwait(false);
        }

        private async Task KeepAliveLoopAsync(CancellationToken token)
        {
            // Track when we last expected to send a keep-alive pong so we can detect a stalled
            // cadence (Task.Delay returning late, ThreadPool starvation, etc.).
            long expectedNextTickMs = McpDiagnosticHooks.MonotonicMs() + (long)_keepAliveInterval.TotalMilliseconds;
            while (!token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_keepAliveInterval, token).ConfigureAwait(false);
                    long actualTickMs = McpDiagnosticHooks.MonotonicMs();
                    long overdueMs = actualTickMs - expectedNextTickMs;
                    if (overdueMs > _keepAliveInterval.TotalMilliseconds)
                    {
                        McpDiagnosticLog.Warn(
                            "WS",
                            $"keep-alive tick overdue by {overdueMs}ms (expected interval={_keepAliveInterval.TotalSeconds}s) {McpDiagnosticHooks.GetHealthSnapshot()}");
                    }
                    expectedNextTickMs = actualTickMs + (long)_keepAliveInterval.TotalMilliseconds;

                    if (_socket == null || _socket.State != WebSocketState.Open)
                    {
                        McpDiagnosticLog.Trace("WS", $"keep-alive loop exiting socketState={_socket?.State.ToString() ?? "null"}");
                        break;
                    }

                    // If a previous send happened, warn when the gap exceeds the staleness threshold.
                    long lastPongTicks = Interlocked.Read(ref _lastPongSentTicks);
                    if (lastPongTicks != 0)
                    {
                        long gapMs = actualTickMs - lastPongTicks;
                        long staleThresholdMs = (long)_keepAliveInterval.TotalMilliseconds * PongStalenessMultiplier;
                        if (gapMs > staleThresholdMs)
                        {
                            McpDiagnosticLog.Warn(
                                "WS",
                                $"no pong sent for {gapMs}ms (threshold={staleThresholdMs}ms) {McpDiagnosticHooks.GetHealthSnapshot()}");
                        }
                    }

                    await SendPongAsync(token, "keepalive").ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    McpLog.Warn($"[WebSocket] Keep-alive failed: {ex.Message}");
                    McpDiagnosticLog.Exception("WS", $"keep-alive failed {McpDiagnosticHooks.GetHealthSnapshot()}", ex);
                    await HandleSocketClosureAsync(ex.Message).ConfigureAwait(false);
                    break;
                }
            }
        }

        private async Task HeartbeatLoopAsync(CancellationToken token)
        {
            McpDiagnosticLog.Trace("Health", "heartbeat loop started");
            try
            {
                while (!token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(HeartbeatInterval, token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    long now = McpDiagnosticHooks.MonotonicMs();
                    long lastPing = Interlocked.Read(ref _lastPingRecvTicks);
                    long lastPong = Interlocked.Read(ref _lastPongSentTicks);
                    long pingAgo = lastPing == 0 ? -1L : (now - lastPing);
                    long pongAgo = lastPong == 0 ? -1L : (now - lastPong);
                    string socketState = _socket?.State.ToString() ?? "null";

                    McpDiagnosticLog.Info(
                        "Health",
                        $"socket={socketState} session={(_sessionId ?? "(none)")} lastPingRecvAgoMs={pingAgo} lastPongSentAgoMs={pongAgo} keepAlive={_keepAliveInterval.TotalSeconds}s reconnecting={(Interlocked.CompareExchange(ref _isReconnectingFlag, 0, 0) == 1 ? "true" : "false")} {McpDiagnosticHooks.GetHealthSnapshot()}");
                }
            }
            catch (Exception ex)
            {
                McpDiagnosticLog.Exception("Health", "heartbeat loop unexpected error", ex);
            }
            finally
            {
                McpDiagnosticLog.Trace("Health", "heartbeat loop ended");
            }
        }

        private async Task SendRegisterAsync(CancellationToken token)
        {
            var registerPayload = new JObject
            {
                ["type"] = "register",
                // session_id is now server-authoritative; omitted here or sent as null
                ["project_name"] = _projectName,
                ["project_hash"] = _projectHash,
                ["unity_version"] = _unityVersion,
                ["project_path"] = _projectPath
            };

            McpDiagnosticLog.Info("Send", $"register project={_projectName} hash={_projectHash}");
            await SendJsonAsync(registerPayload, token).ConfigureAwait(false);
        }

        private async Task SendPongAsync(CancellationToken token, string reason)
        {
            var payload = new JObject
            {
                ["type"] = "pong",
                ["session_id"] = _sessionId  // Include session ID for server-side tracking
            };

            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                await SendJsonAsync(payload, token).ConfigureAwait(false);
                sw.Stop();
                Interlocked.Exchange(ref _lastPongSentTicks, McpDiagnosticHooks.MonotonicMs());
                if (sw.ElapsedMilliseconds >= SlowSendWarnMs)
                {
                    McpDiagnosticLog.Warn("Send", $"pong reason={reason} ms={sw.ElapsedMilliseconds} (slow) {McpDiagnosticHooks.GetHealthSnapshot()}");
                }
                else
                {
                    McpDiagnosticLog.Trace("Send", $"pong reason={reason} ms={sw.ElapsedMilliseconds}");
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                sw.Stop();
                McpDiagnosticLog.Warn("Send", $"pong reason={reason} FAILED after {sw.ElapsedMilliseconds}ms: {ex.GetType().Name}: {ex.Message}");
                throw;
            }
        }

        private async Task SendJsonAsync(JObject payload, CancellationToken token)
        {
            if (_socket == null)
            {
                throw new InvalidOperationException("WebSocket is not initialised");
            }

            string json = payload.ToString(Formatting.None);
            byte[] bytes = Encoding.UTF8.GetBytes(json);
            var buffer = new ArraySegment<byte>(bytes);

            await _sendLock.WaitAsync(token).ConfigureAwait(false);
            try
            {
                if (_socket.State != WebSocketState.Open)
                {
                    throw new InvalidOperationException("WebSocket is not open");
                }

                await _socket.SendAsync(buffer, WebSocketMessageType.Text, true, token).ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }

        private async Task HandleSocketClosureAsync(string reason)
        {
            // Capture stack trace for debugging disconnection triggers
            var stackTrace = new System.Diagnostics.StackTrace(true);
            McpLog.Debug($"[WebSocket] HandleSocketClosureAsync called. Reason: {reason}\nStack trace:\n{stackTrace}");
            McpDiagnosticLog.Info("WS", $"socket closure reason='{reason}' session={_sessionId ?? "(none)"} socketState={_socket?.State.ToString() ?? "null"}");

            if (_lifecycleCts == null || _lifecycleCts.IsCancellationRequested)
            {
                McpDiagnosticLog.Trace("WS", "socket closure: lifecycle already cancelled — not reconnecting");
                return;
            }

            if (Interlocked.CompareExchange(ref _isReconnectingFlag, 1, 0) != 0)
            {
                McpDiagnosticLog.Trace("WS", "socket closure: reconnect already in progress — skipping");
                return;
            }

            _isConnected = false;
            _state = _state.WithError(reason ?? "Connection closed");
            McpLog.Warn($"[WebSocket] Connection closed: {reason}");

            await StopConnectionLoopsAsync(awaitTasks: false).ConfigureAwait(false);

            _ = Task.Run(() => AttemptReconnectAsync(_lifecycleCts.Token), CancellationToken.None);
        }

        private async Task AttemptReconnectAsync(CancellationToken token)
        {
            McpDiagnosticLog.Info("WS", "AttemptReconnectAsync: starting reconnect schedule");
            try
            {
                await StopConnectionLoopsAsync().ConfigureAwait(false);

                int attempt = 0;
                foreach (TimeSpan delay in ReconnectSchedule)
                {
                    attempt++;
                    if (token.IsCancellationRequested)
                    {
                        McpDiagnosticLog.Info("WS", $"AttemptReconnectAsync: cancelled before attempt {attempt}");
                        return;
                    }

                    if (delay > TimeSpan.Zero)
                    {
                        try { await Task.Delay(delay, token).ConfigureAwait(false); }
                        catch (OperationCanceledException) { McpDiagnosticLog.Info("WS", $"AttemptReconnectAsync: cancelled during {delay.TotalSeconds}s backoff"); return; }
                    }

                    McpDiagnosticLog.Info("WS", $"AttemptReconnectAsync: attempt {attempt}/{ReconnectSchedule.Length}");
                    if (await EstablishConnectionAsync(token).ConfigureAwait(false))
                    {
                        _state = TransportState.Connected(TransportDisplayName, sessionId: _sessionId, details: _endpointUri.ToString());
                        _isConnected = true;
                        McpLog.Info("[WebSocket] Reconnected to MCP server", false);
                        McpDiagnosticLog.Info("WS", $"AttemptReconnectAsync: succeeded on attempt {attempt}");
                        return;
                    }
                    McpDiagnosticLog.Warn("WS", $"AttemptReconnectAsync: attempt {attempt} failed");
                }
                McpDiagnosticLog.Warn("WS", "AttemptReconnectAsync: initial schedule exhausted; falling back to tail interval");

                // Schedule exhausted — keep retrying every 30 s indefinitely so a transient
                // server outage longer than ~49 s doesn't leave the plugin permanently dead.
                McpLog.Warn($"[WebSocket] Initial reconnect schedule exhausted. Retrying every {ReconnectTailInterval.TotalSeconds}s until cancelled.");
                _state = _state.WithError($"Server unreachable – retrying every {ReconnectTailInterval.TotalSeconds} s");
                while (!token.IsCancellationRequested)
                {
                    try { await Task.Delay(ReconnectTailInterval, token).ConfigureAwait(false); }
                    catch (OperationCanceledException) { return; }

                    if (await EstablishConnectionAsync(token).ConfigureAwait(false))
                    {
                        _state = TransportState.Connected(TransportDisplayName, sessionId: _sessionId, details: _endpointUri.ToString());
                        _isConnected = true;
                        McpLog.Info("[WebSocket] Reconnected to MCP server", false);
                        return;
                    }
                }
            }
            finally
            {
                Interlocked.Exchange(ref _isReconnectingFlag, 0);
            }
        }

        private static Uri BuildWebSocketUri(string baseUrl)
        {
            if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var httpUri))
            {
                throw new InvalidOperationException($"Invalid MCP base URL: {baseUrl}");
            }

            // Replace bind-only addresses for client connections
            // 0.0.0.0 and :: are only valid for server binding, not client connections
            string host = httpUri.Host;
            if (host == "0.0.0.0")
            {
                McpLog.Warn($"[WebSocket] Base URL host '{host}' is bind-only; using '127.0.0.1' for client connection.");
                host = "127.0.0.1";
            }
            else if (host == "::")
            {
                McpLog.Warn($"[WebSocket] Base URL host '{host}' is bind-only; using '::1' for client connection.");
                host = "::1";
            }

            var builder = new UriBuilder(httpUri)
            {
                Scheme = httpUri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase) ? "wss" : "ws",
                Host = host,
                Path = httpUri.AbsolutePath.TrimEnd('/') + "/hub/plugin"
            };

            return builder.Uri;
        }

        private static List<Uri> BuildConnectionCandidateUris(Uri endpointUri)
        {
            var candidates = new List<Uri>();
            if (endpointUri == null)
            {
                return candidates;
            }

            candidates.Add(endpointUri);

            if (!string.Equals(endpointUri.Host, "localhost", StringComparison.OrdinalIgnoreCase))
            {
                return candidates;
            }

            // Retry localhost using explicit loopback hosts to avoid DNS family ambiguity on some machines.
            TryAddCandidate(candidates, endpointUri, "127.0.0.1");
            TryAddCandidate(candidates, endpointUri, "::1");
            return candidates;
        }

        private static void TryAddCandidate(List<Uri> candidates, Uri template, string host)
        {
            try
            {
                var builder = new UriBuilder(template) { Host = host };
                Uri candidate = builder.Uri;
                foreach (Uri existing in candidates)
                {
                    if (Uri.Compare(existing, candidate, UriComponents.AbsoluteUri, UriFormat.SafeUnescaped, StringComparison.OrdinalIgnoreCase) == 0)
                    {
                        return;
                    }
                }
                candidates.Add(candidate);
            }
            catch
            {
                // Ignore malformed fallback candidate and continue with remaining options.
            }
        }
    }
}
