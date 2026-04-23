using System;
using System.Threading.Tasks;
using MCPForUnity.Editor.Constants;
using MCPForUnity.Editor.Helpers;
using MCPForUnity.Editor.Services.Transport;
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

        // Used after a successful server revive in KeepRunning mode — short reconnect-only schedule.
        private static readonly TimeSpan[] ReviveReconnectSchedule =
        {
            TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(3),
            TimeSpan.FromSeconds(5)
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
                bool isRunning = transport.IsRunning(TransportMode.Http);
                bool keepRunning = AutoStartPolicySettings.Get() == AutoStartPolicy.KeepRunning;

                // In KeepRunning the post-reload handler must fire even when the bridge was idle
                // before the reload, so revive can bring the server back.
                bool shouldResume = isRunning || keepRunning;

                if (shouldResume)
                {
                    EditorPrefs.SetBool(EditorPrefKeys.ResumeHttpAfterReload, true);
                }
                else
                {
                    EditorPrefs.DeleteKey(EditorPrefKeys.ResumeHttpAfterReload);
                }

                if (isRunning)
                {
                    // beforeAssemblyReload is synchronous; force a synchronous teardown so we do not
                    // leave an orphaned socket due to an unfinished async close handshake.
                    transport.ForceStop(TransportMode.Http);
                }
            }
            catch (Exception ex)
            {
                McpLog.Warn($"Failed to evaluate HTTP bridge reload state: {ex.Message}");
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
                if (resume)
                {
                    EditorPrefs.DeleteKey(EditorPrefKeys.ResumeHttpAfterReload);
                }
            }
            catch (Exception ex)
            {
                McpLog.Warn($"Failed to read HTTP bridge reload flag: {ex.Message}");
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
            // KeepRunning fast path: server already known dead — skip the ~49s retry loop and revive directly.
            if (AutoStartPolicySettings.Get() == AutoStartPolicy.KeepRunning && !MCPServiceLocator.Server.IsLocalHttpServerReachable())
            {
                // Suppress both revive and the "Failed to resume" warning if the user opted out —
                // the warning would be misleading when nothing is actually failing.
                if (AutoStartPolicySettings.IsSessionEndedByUser())
                {
                    return;
                }

                if (await TryReviveServerAsync())
                {
                    return;
                }

                McpLog.Warn("Failed to resume HTTP MCP bridge after domain reload");
                return;
            }

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
                        MCPForUnityEditorWindow.RequestHealthVerification();
                        return;
                    }

                    var state = MCPServiceLocator.TransportManager.GetState(TransportMode.Http);
                    string reason = string.IsNullOrWhiteSpace(state?.Error) ? "no error detail" : state.Error;
                    McpLog.Debug($"[HTTP Reload] Resume attempt {attempt} failed: {reason}");
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    McpLog.Debug($"[HTTP Reload] Resume attempt {attempt} threw: {ex.Message}");
                }
            }

            if (AutoStartPolicySettings.Get() == AutoStartPolicy.KeepRunning && await TryReviveServerAsync())
            {
                return;
            }

            if (lastException != null)
            {
                McpLog.Warn($"Failed to resume HTTP MCP bridge after domain reload: {lastException.Message}");
            }
            else
            {
                McpLog.Warn("Failed to resume HTTP MCP bridge after domain reload");
            }
        }

        /// <summary>
        /// KeepRunning fallback: launch the local HTTP server (if not already up) and try a short
        /// reconnect schedule. Returns true if the bridge becomes connected.
        /// </summary>
        private static async Task<bool> TryReviveServerAsync()
        {
            if (AutoStartPolicySettings.IsSessionEndedByUser())
            {
                return false;
            }

            // Abort if the user switched transports while we were running the original retry loop.
            if (!EditorConfigurationCache.Instance.UseHttpTransport)
            {
                return false;
            }

            if (HttpEndpointUtility.IsRemoteScope())
            {
                return false;
            }

            // Server may have come up via another path (e.g. user clicked Start Server) — don't double-launch.
            if (!MCPServiceLocator.Server.IsLocalHttpServerReachable())
            {
                McpLog.Info("[HTTP KeepRunning] Reviving local HTTP server after failed reconnect cycle");
                bool started = MCPServiceLocator.Server.StartLocalHttpServer(quiet: true);
                if (!started)
                {
                    McpLog.Warn("[HTTP KeepRunning] Failed to launch local HTTP server");
                    return false;
                }
            }

            for (int i = 0; i < ReviveReconnectSchedule.Length; i++)
            {
                int attempt = i + 1;
                TimeSpan delay = ReviveReconnectSchedule[i];
                McpLog.Debug($"[HTTP KeepRunning] Waiting {delay.TotalSeconds:0.#}s before revive-reconnect attempt {attempt}");
                try { await Task.Delay(delay); }
                catch { return false; }

                if (!EditorConfigurationCache.Instance.UseHttpTransport)
                {
                    return false;
                }

                try
                {
                    bool connected = await MCPServiceLocator.TransportManager.StartAsync(TransportMode.Http);
                    if (connected)
                    {
                        McpLog.Info("[HTTP KeepRunning] Bridge restored after server revive");
                        MCPForUnityEditorWindow.RequestHealthVerification();
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    McpLog.Debug($"[HTTP KeepRunning] Revive-reconnect attempt {attempt} threw: {ex.Message}");
                }
            }

            return false;
        }
    }
}
