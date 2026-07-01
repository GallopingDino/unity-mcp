using System;
using System.Threading.Tasks;
using MCPForUnity.Editor.Constants;
using MCPForUnity.Editor.Helpers;
using MCPForUnity.Editor.Services.Transport;
using MCPForUnity.Editor.Services.Transport.Transports;
using MCPForUnity.Editor.Windows;
using UnityEditor;

namespace MCPForUnity.Editor.Services
{
    /// <summary>
    /// Ensures HTTP transports resume after domain reloads similar to the legacy stdio bridge.
    /// </summary>
    [InitializeOnLoad]
    internal static class HttpBridgeReloadHandler
    {
        private static readonly TimeSpan[] ResumeRetrySchedule =
        {
            TimeSpan.Zero,
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(3),
            TimeSpan.FromSeconds(5),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(30)
        };

        static HttpBridgeReloadHandler()
        {
            AssemblyReloadEvents.beforeAssemblyReload += OnBeforeAssemblyReload;
            AssemblyReloadEvents.afterAssemblyReload += OnAfterAssemblyReload;
        }

        private static void OnBeforeAssemblyReload()
        {
            try
            {
                var transport = MCPServiceLocator.TransportManager;
                var httpClient = transport.GetClient(TransportMode.Http);

                // TransportManager._httpState is updated only when the StartAsync continuation
                // resumes on the captured SynchronizationContext. If beforeAssemblyReload fires
                // while that continuation is still queued (e.g. a second domain reload arriving
                // 1-2s after the first reconnect), the manager-level flag is still false even
                // though the WebSocket is already open and the session is registered. Fall back
                // to the client's own liveness so we still send expect_reconnect in that window.
                bool managerRunning = transport.IsRunning(TransportMode.Http);
                bool clientHasLiveSession = httpClient is WebSocketTransportClient liveWs && liveWs.HasLiveSession;
                bool shouldResume = managerRunning || clientHasLiveSession;
                McpDiagnosticLog.Info(
                    "Resume",
                    $"OnBeforeAssemblyReload httpRunning={managerRunning} hasLiveSession={clientHasLiveSession} shouldResume={shouldResume}");

                if (shouldResume)
                {
                    EditorPrefs.SetBool(EditorPrefKeys.ResumeHttpAfterReload, true);

                    // Tell the server we're reloading so it holds the connection slot open
                    // for the reconnect (instead of treating this disconnect as a session end).
                    if (httpClient is WebSocketTransportClient ws)
                    {
                        try
                        {
                            bool sent = ws.TrySendExpectReconnectSync(reason: "domain_reload", timeoutMs: 200);
                            McpDiagnosticLog.Info("Resume", $"expect_reconnect send result={sent} welcome={ws.WelcomeReceived}");
                        }
                        catch (Exception ex)
                        {
                            McpLog.Debug($"expect_reconnect send failed; server will fall back to session-end on disconnect: {ex.Message}");
                            McpDiagnosticLog.Exception("Resume", "expect_reconnect threw", ex);
                        }
                    }
                    else
                    {
                        McpDiagnosticLog.Warn("Resume", "OnBeforeAssemblyReload: HTTP client is not WebSocketTransportClient — cannot send expect_reconnect");
                    }

                    // beforeAssemblyReload is synchronous; force a synchronous teardown so we do not
                    // leave an orphaned socket due to an unfinished async close handshake.
                    McpDiagnosticLog.Info("Resume", "calling ForceStop(Http) to abort socket synchronously");
                    transport.ForceStop(TransportMode.Http);
                }
                else
                {
                    EditorPrefs.DeleteKey(EditorPrefKeys.ResumeHttpAfterReload);
                }
            }
            catch (Exception ex)
            {
                McpLog.Warn($"Failed to evaluate HTTP bridge reload state: {ex.Message}");
                McpDiagnosticLog.Exception("Resume", "OnBeforeAssemblyReload threw", ex);
            }
        }

        private static void OnAfterAssemblyReload()
        {
            bool resume = false;
            try
            {
                // Only resume HTTP if it is still the selected transport.
                bool useHttp = EditorConfigurationCache.Instance.UseHttpTransport;
                resume = useHttp && EditorPrefs.GetBool(EditorPrefKeys.ResumeHttpAfterReload, false);
                McpDiagnosticLog.Info("Resume", $"OnAfterAssemblyReload useHttp={useHttp} resumeFlag={resume}");
                if (resume)
                {
                    EditorPrefs.DeleteKey(EditorPrefKeys.ResumeHttpAfterReload);
                }
            }
            catch (Exception ex)
            {
                McpLog.Warn($"Failed to read HTTP bridge reload flag: {ex.Message}");
                McpDiagnosticLog.Exception("Resume", "OnAfterAssemblyReload flag read failed", ex);
                resume = false;
            }

            if (!resume)
            {
                return;
            }

            // If the editor is not compiling, attempt an immediate restart without relying on editor focus.
            bool isCompiling = EditorApplication.isCompiling;
            try
            {
                var pipeline = Type.GetType("UnityEditor.Compilation.CompilationPipeline, UnityEditor");
                var prop = pipeline?.GetProperty("isCompiling", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
                if (prop != null) isCompiling |= (bool)prop.GetValue(null);
            }
            catch { }

            if (!isCompiling)
            {
                _ = ResumeHttpWithRetriesAsync();
                return;
            }

            // Fallback when compiling: schedule on the editor loop
            EditorApplication.delayCall += () =>
            {
                _ = ResumeHttpWithRetriesAsync();
            };
        }

        private static async Task ResumeHttpWithRetriesAsync()
        {
            Exception lastException = null;

            for (int i = 0; i < ResumeRetrySchedule.Length; i++)
            {
                int attempt = i + 1;
                McpLog.Debug($"[HTTP Reload] Resume attempt {attempt}/{ResumeRetrySchedule.Length}");

                TimeSpan delay = ResumeRetrySchedule[i];
                if (delay > TimeSpan.Zero)
                {
                    McpLog.Debug($"[HTTP Reload] Waiting {delay.TotalSeconds:0.#}s before resume attempt {attempt}");
                    try { await Task.Delay(delay); }
                    catch { return; }
                }

                // Abort retries if the user switched transports while we were waiting.
                if (!EditorConfigurationCache.Instance.UseHttpTransport)
                {
                    return;
                }

                try
                {
                    bool started = await MCPServiceLocator.TransportManager.StartAsync(TransportMode.Http);
                    if (started)
                    {
                        McpLog.Debug($"[HTTP Reload] Resume succeeded on attempt {attempt}");
                        McpDiagnosticLog.Info("Resume", $"resume succeeded attempt={attempt}");
                        MCPForUnityEditorWindow.RequestHealthVerification();
                        return;
                    }

                    var state = MCPServiceLocator.TransportManager.GetState(TransportMode.Http);
                    string reason = string.IsNullOrWhiteSpace(state?.Error) ? "no error detail" : state.Error;
                    McpLog.Debug($"[HTTP Reload] Resume attempt {attempt} failed: {reason}");
                    McpDiagnosticLog.Warn("Resume", $"resume attempt {attempt} failed: {reason}");
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    McpLog.Debug($"[HTTP Reload] Resume attempt {attempt} threw: {ex.Message}");
                    McpDiagnosticLog.Exception("Resume", $"resume attempt {attempt} threw", ex);
                }
            }

            if (lastException != null)
            {
                McpLog.Warn($"Failed to resume HTTP MCP bridge after domain reload: {lastException.Message}");
                McpDiagnosticLog.Exception("Resume", "all resume attempts exhausted", lastException);
            }
            else
            {
                McpLog.Warn("Failed to resume HTTP MCP bridge after domain reload");
                McpDiagnosticLog.Error("Resume", "all resume attempts exhausted (no exception captured)");
            }
        }
    }
}
