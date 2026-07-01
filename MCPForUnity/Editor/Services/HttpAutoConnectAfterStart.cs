using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using MCPForUnity.Editor.Constants;
using MCPForUnity.Editor.Helpers;
using UnityEditor;

namespace MCPForUnity.Editor.Services
{
    /// <summary>
    /// Owns the "user launched the local HTTP server, now wait for it to become
    /// reachable and open the WebSocket" handoff at a global scope. Survives
    /// MCP-window close and domain reload because the deadline is persisted in
    /// <see cref="EditorPrefKeys.AutoConnectAfterServerStartDeadlineUtc"/> and
    /// the ticker is driven by a static <c>EditorApplication.update</c>
    /// subscription. The window-local <c>EvaluateAutoConnect</c> is kept as
    /// belt-and-suspenders for status display only.
    /// </summary>
    [InitializeOnLoad]
    internal static class HttpAutoConnectAfterStart
    {
        // Mirrors McpConnectionSection.StartingServerTimeoutSeconds. Comfortably
        // inside the server's 300s cold-start grace once uvx has finished
        // building/launching the process.
        private const double DeadlineWindowSeconds = 180.0;
        private const double PollIntervalSeconds = 2.0;
        // The WebSocket endpoint may come up a beat after the TCP probe succeeds;
        // matches ConnectAfterServerReadyAsync.
        private const int PostProbeGraceMs = 1000;

        private static double _lastPollTime;
        private static int _connectInFlight;

        static HttpAutoConnectAfterStart()
        {
            EditorApplication.update += OnEditorUpdate;
        }

        internal static void Arm()
        {
            long deadlineUnix = DateTimeOffset.UtcNow.AddSeconds(DeadlineWindowSeconds).ToUnixTimeSeconds();
            EditorPrefs.SetString(
                EditorPrefKeys.AutoConnectAfterServerStartDeadlineUtc,
                deadlineUnix.ToString(CultureInfo.InvariantCulture));
            _lastPollTime = 0;
            McpDiagnosticLog.Info("AutoConnect", $"armed deadline={deadlineUnix} (+{DeadlineWindowSeconds:0}s)");
        }

        internal static void Disarm(string reason)
        {
            if (!EditorPrefs.HasKey(EditorPrefKeys.AutoConnectAfterServerStartDeadlineUtc))
            {
                return;
            }
            EditorPrefs.DeleteKey(EditorPrefKeys.AutoConnectAfterServerStartDeadlineUtc);
            McpDiagnosticLog.Info("AutoConnect", $"disarmed reason={reason}");
        }

        private static void OnEditorUpdate()
        {
            if (!EditorPrefs.HasKey(EditorPrefKeys.AutoConnectAfterServerStartDeadlineUtc))
            {
                return;
            }

            double now = EditorApplication.timeSinceStartup;
            if (now - _lastPollTime < PollIntervalSeconds)
            {
                return;
            }
            _lastPollTime = now;

            string raw = EditorPrefs.GetString(EditorPrefKeys.AutoConnectAfterServerStartDeadlineUtc, string.Empty);
            if (string.IsNullOrEmpty(raw)
                || !long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out long deadlineUnix))
            {
                Disarm("invalid deadline value");
                return;
            }

            if (DateTimeOffset.UtcNow.ToUnixTimeSeconds() >= deadlineUnix)
            {
                Disarm("deadline expired");
                return;
            }

            if (!EditorConfigurationCache.Instance.UseHttpTransport)
            {
                Disarm("transport switched away from HTTP");
                return;
            }

            bool reachable;
            try
            {
                reachable = MCPServiceLocator.Server.IsLocalHttpServerReachable();
            }
            catch (Exception ex)
            {
                McpDiagnosticLog.Exception("AutoConnect", "reachability probe failed", ex);
                return;
            }

            if (!reachable)
            {
                return;
            }

            if (Interlocked.CompareExchange(ref _connectInFlight, 1, 0) != 0)
            {
                return;
            }

            _ = ConnectAsync();
        }

        private static async Task ConnectAsync()
        {
            try
            {
                await Task.Delay(PostProbeGraceMs);
                McpDiagnosticLog.Info("AutoConnect", "server reachable; calling Bridge.StartAsync");
                bool started = await MCPServiceLocator.Bridge.StartAsync();
                if (started)
                {
                    Disarm("connect succeeded");
                }
                else
                {
                    McpDiagnosticLog.Warn("AutoConnect", "Bridge.StartAsync returned false; will retry next tick");
                }
            }
            catch (Exception ex)
            {
                McpDiagnosticLog.Exception("AutoConnect", "Bridge.StartAsync threw", ex);
            }
            finally
            {
                Interlocked.Exchange(ref _connectInFlight, 0);
            }
        }
    }
}
