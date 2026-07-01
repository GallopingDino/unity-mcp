using System;
using System.Threading.Tasks;
using MCPForUnity.Editor.Helpers;
using MCPForUnity.Editor.Services.Transport;
using MCPForUnity.Editor.Services.Transport.Transports;
using UnityEditor;

namespace MCPForUnity.Editor.Services
{
    /// <summary>
    /// Best-effort cleanup when the Unity Editor is quitting; stops active transports so the server sees a clean disconnect.
    /// </summary>
    [InitializeOnLoad]
    internal static class McpEditorShutdownCleanup
    {
        static McpEditorShutdownCleanup()
        {
            // Guard against duplicate subscriptions across domain reloads.
            try { EditorApplication.quitting -= OnEditorQuitting; } catch { }
            EditorApplication.quitting += OnEditorQuitting;
        }

        private static void OnEditorQuitting()
        {
            McpDiagnosticLog.Info("Quit", "shutdown cleanup: announcing session_end then stopping transports (Http/Stdio)");
            var sw = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                var transport = MCPServiceLocator.TransportManager;

                // Announce clean shutdown to the server so it can skip the
                // transport-recovery grace it would otherwise apply to the
                // 1005 close that the editor process exit usually produces
                // (the WebSocket close-frame frequently fails to flush
                // before the OS tears the socket down).
                try
                {
                    if (transport.GetClient(TransportMode.Http) is WebSocketTransportClient ws)
                    {
                        bool sent = ws.TrySendSessionEndSync("editor_quit");
                        McpDiagnosticLog.Info("Quit", $"session_end send result={sent} welcome={ws.WelcomeReceived}");
                    }
                }
                catch (Exception ex)
                {
                    McpDiagnosticLog.Exception("Quit", "session_end send threw", ex);
                }

                Task stopHttp = transport.StopAsync(TransportMode.Http);
                Task stopStdio = transport.StopAsync(TransportMode.Stdio);

                bool completed = false;
                try { completed = Task.WaitAll(new[] { stopHttp, stopStdio }, 750); } catch (Exception ex) { McpDiagnosticLog.Exception("Quit", "transport stop wait failed", ex); }
                sw.Stop();
                McpDiagnosticLog.Info("Quit", $"shutdown cleanup: transports stopped completed={completed} ms={sw.ElapsedMilliseconds}");
            }
            catch (Exception ex)
            {
                McpLog.Warn($"Shutdown cleanup: failed to stop transports: {ex.Message}");
                McpDiagnosticLog.Exception("Quit", "shutdown cleanup failed", ex);
            }
        }
    }
}

