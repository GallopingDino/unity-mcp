using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using MCPForUnity.Editor.Helpers;
using UnityEditor;
using UnityEditor.Compilation;
using UnityEditorInternal;
using UnityEngine;

namespace MCPForUnity.Editor.Services
{
    /// <summary>
    /// Subscribes to Editor lifecycle events (assembly reload, compilation, quit, play mode)
    /// and forwards them to <see cref="McpDiagnosticLog"/> so transport disconnects can be
    /// correlated with editor activity post-mortem.
    /// Also samples a thread-safe snapshot of editor state from the main thread so background
    /// loops (e.g. WebSocket keep-alive) can include it in heartbeat logs without touching
    /// main-thread-only APIs.
    /// </summary>
    [InitializeOnLoad]
    internal static class McpDiagnosticHooks
    {
        // Snapshot fields. Written from EditorApplication.update / lifecycle callbacks (main thread),
        // read from any thread via GetHealthSnapshot. Long reads use Interlocked for 32-bit safety.
        private static long _lastEditorUpdateTicks;
        private static volatile bool _isCompiling;
        private static volatile bool _isPlaying;
        private static volatile bool _isPaused;
        private static volatile bool _isFocused;
        private static volatile bool _runInBackground;

        static McpDiagnosticHooks()
        {
            McpDiagnosticLog.Init();
            McpDiagnosticLog.Info("Boot", "domain loaded; subscribing to editor lifecycle events");

            try { AssemblyReloadEvents.beforeAssemblyReload -= OnBeforeReload; } catch { }
            AssemblyReloadEvents.beforeAssemblyReload += OnBeforeReload;
            try { AssemblyReloadEvents.afterAssemblyReload -= OnAfterReload; } catch { }
            AssemblyReloadEvents.afterAssemblyReload += OnAfterReload;

            try { EditorApplication.quitting -= OnQuitting; } catch { }
            EditorApplication.quitting += OnQuitting;
            try { EditorApplication.wantsToQuit -= OnWantsToQuit; } catch { }
            EditorApplication.wantsToQuit += OnWantsToQuit;

            try { CompilationPipeline.compilationStarted -= OnCompilationStarted; } catch { }
            CompilationPipeline.compilationStarted += OnCompilationStarted;
            try { CompilationPipeline.compilationFinished -= OnCompilationFinished; } catch { }
            CompilationPipeline.compilationFinished += OnCompilationFinished;
            try { CompilationPipeline.assemblyCompilationStarted -= OnAssemblyCompilationStarted; } catch { }
            CompilationPipeline.assemblyCompilationStarted += OnAssemblyCompilationStarted;
            try { CompilationPipeline.assemblyCompilationFinished -= OnAssemblyCompilationFinished; } catch { }
            CompilationPipeline.assemblyCompilationFinished += OnAssemblyCompilationFinished;

            try { EditorApplication.playModeStateChanged -= OnPlayModeStateChanged; } catch { }
            EditorApplication.playModeStateChanged += OnPlayModeStateChanged;
            try { EditorApplication.pauseStateChanged -= OnPauseStateChanged; } catch { }
            EditorApplication.pauseStateChanged += OnPauseStateChanged;

            try { EditorApplication.update -= OnEditorUpdate; } catch { }
            EditorApplication.update += OnEditorUpdate;

            // Seed snapshot immediately so the first heartbeat has something useful even
            // before the editor fires its first update tick.
            try { OnEditorUpdate(); } catch { }
        }

        /// <summary>
        /// Monotonic millisecond clock built on <see cref="Stopwatch"/>. Safe across all
        /// supported Unity / .NET targets (no <c>Environment.TickCount64</c> dependency).
        /// </summary>
        public static long MonotonicMs()
        {
            // Stopwatch.Frequency is normally 10_000_000 (100 ns). Multiply first only if it
            // would not overflow long; the simple form below is exact enough at ms granularity
            // and never overflows within any sane process uptime.
            return Stopwatch.GetTimestamp() / (Stopwatch.Frequency / 1000L);
        }

        /// <summary>
        /// Returns a one-line, allocation-cheap snapshot of editor state. Safe from any thread.
        /// <c>lastUpdateAgoMs=-1</c> means the editor update tick has never been observed.
        /// </summary>
        public static string GetHealthSnapshot()
        {
            long lastUpdate = Interlocked.Read(ref _lastEditorUpdateTicks);
            long ageMs = lastUpdate == 0 ? -1L : (MonotonicMs() - lastUpdate);
            return string.Format(
                CultureInfo.InvariantCulture,
                "lastUpdateAgoMs={0} compiling={1} playing={2} paused={3} focused={4} runInBg={5}",
                ageMs,
                _isCompiling ? "true" : "false",
                _isPlaying ? "true" : "false",
                _isPaused ? "true" : "false",
                _isFocused ? "true" : "false",
                _runInBackground ? "true" : "false");
        }

        private static void OnEditorUpdate()
        {
            // Keep this hot path tight: a few field writes per tick, no allocations, no logging.
            Interlocked.Exchange(ref _lastEditorUpdateTicks, MonotonicMs());
            try
            {
                _isCompiling = EditorApplication.isCompiling;
                _isPlaying = EditorApplication.isPlaying;
                _isPaused = EditorApplication.isPaused;
                _runInBackground = Application.runInBackground;
                // EditorApplication.focusChanged is not present on every supported Unity
                // version, so poll the stable internal flag instead. It only flips when the
                // editor regains/loses focus, so reading it on every tick is cheap.
                _isFocused = InternalEditorUtility.isApplicationActive;
            }
            catch
            {
                // Should never throw, but guarantee the tick timestamp still advances.
            }
        }

        private static void OnBeforeReload()
        {
            McpDiagnosticLog.Info("Reload", "beforeAssemblyReload");
        }

        private static void OnAfterReload()
        {
            McpDiagnosticLog.Info("Reload", "afterAssemblyReload");
        }

        private static void OnQuitting()
        {
            McpDiagnosticLog.Info("Quit", "EditorApplication.quitting");
        }

        private static bool OnWantsToQuit()
        {
            McpDiagnosticLog.Info("Quit", "EditorApplication.wantsToQuit");
            return true;
        }

        private static void OnCompilationStarted(object context)
        {
            McpDiagnosticLog.Info("Compile", "compilationStarted context=" + (context ?? "null"));
        }

        private static void OnCompilationFinished(object context)
        {
            McpDiagnosticLog.Info("Compile", "compilationFinished context=" + (context ?? "null"));
        }

        private static void OnAssemblyCompilationStarted(string assemblyPath)
        {
            McpDiagnosticLog.Trace("Compile", "asm started " + (assemblyPath ?? string.Empty));
        }

        private static void OnAssemblyCompilationFinished(string assemblyPath, CompilerMessage[] messages)
        {
            int errors = 0;
            int warnings = 0;
            if (messages != null)
            {
                for (int i = 0; i < messages.Length; i++)
                {
                    if (messages[i].type == CompilerMessageType.Error) errors++;
                    else if (messages[i].type == CompilerMessageType.Warning) warnings++;
                }
            }
            McpDiagnosticLog.Trace(
                "Compile",
                "asm finished " + (assemblyPath ?? string.Empty) + " errors=" + errors + " warnings=" + warnings);
        }

        private static void OnPlayModeStateChanged(PlayModeStateChange state)
        {
            McpDiagnosticLog.Info("PlayMode", state.ToString());
        }

        private static void OnPauseStateChanged(PauseState state)
        {
            McpDiagnosticLog.Info("PlayMode", "pause=" + state);
        }
    }
}
