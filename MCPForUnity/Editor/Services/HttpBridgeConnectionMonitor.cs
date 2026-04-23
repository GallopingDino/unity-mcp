using System;
using System.Threading;
using System.Threading.Tasks;
using MCPForUnity.Editor.Constants;
using MCPForUnity.Editor.Helpers;
using UnityEditor;
using UnityEngine;

namespace MCPForUnity.Editor.Services
{
    /// <summary>
    /// Revives the local HTTP server when WebSocket reconnect schedule is exhausted,
    /// but only under AutoStartPolicy.KeepRunning for a local-scope HTTP transport.
    /// Fires from WebSocket's own reconnect loop — no independent tick/timer.
    /// </summary>
    [InitializeOnLoad]
    internal static class HttpBridgeConnectionMonitor
    {
        static HttpBridgeConnectionMonitor()
        {
            if (Application.isBatchMode &&
                string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("UNITY_MCP_ALLOW_BATCH")))
            {
                return;
            }

            EditorApplication.delayCall += AttachFallback;
        }

        private static void AttachFallback()
        {
            try
            {
                MCPServiceLocator.TransportManager.SetHttpReconnectFallback(TryReviveServerAsync);
            }
            catch (Exception ex)
            {
                // Loud intentionally: attach failure leaves the monitor inert with no signal
                // other than "revive never fires" — that's a diagnostic dead end at runtime.
                McpLog.Warn($"[HTTP Monitor] Failed to attach reconnect fallback; server revive will not work: {ex.Message}");
            }
        }

        internal static Task TryReviveServerAsync(CancellationToken token)
        {
            if (AutoStartPolicySettings.Get() != AutoStartPolicy.KeepRunning) return Task.CompletedTask;
            if (AutoStartPolicySettings.IsSessionEndedByUser()) return Task.CompletedTask;
            if (!EditorConfigurationCache.Instance.UseHttpTransport) return Task.CompletedTask;
            if (HttpEndpointUtility.IsRemoteScope()) return Task.CompletedTask;
            if (MCPServiceLocator.Server.IsLocalHttpServerReachable()) return Task.CompletedTask;

            McpLog.Info("[HTTP Monitor] WebSocket reconnect schedule exhausted; reviving local HTTP server");
            bool started = MCPServiceLocator.Server.StartLocalHttpServer(quiet: true);
            if (!started)
            {
                McpLog.Warn("[HTTP Monitor] Failed to launch local HTTP server");
            }
            return Task.CompletedTask;
        }
    }
}
