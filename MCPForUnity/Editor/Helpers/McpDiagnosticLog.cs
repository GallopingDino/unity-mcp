using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using MCPForUnity.Editor.Constants;
using UnityEditor;
using UnityEngine;

namespace MCPForUnity.Editor.Helpers
{
    /// <summary>
    /// Append-only diagnostic file log for MCP-for-Unity transport and editor lifecycle events.
    /// Writes to <c>&lt;projectRoot&gt;/Library/MCPForUnity/Logs/diagnostic.log</c>, outside <c>Assets/</c>
    /// so Unity does not import it. Survives domain reloads and editor restarts.
    /// </summary>
    internal static class McpDiagnosticLog
    {
        public enum Level { Trace, Info, Warn, Error }

        private const long MaxBytes = 8L * 1024 * 1024;
        private const int MaxMessageChars = 8192;

        private static readonly object _lock = new();
        private static string _logPath;
        private static int _mainThreadId = -1;
        private static int _processId;
        private static bool _initialized;
        private static volatile bool _enabled;
        private static volatile bool _failed;

        public static bool IsEnabled => _enabled && _initialized && !_failed;
        public static string LogFilePath => _logPath;

        /// <summary>
        /// Idempotent. Call from a main-thread <c>[InitializeOnLoad]</c> hook before any
        /// background thread tries to log, so the log path and main thread id are captured.
        /// </summary>
        public static void Init()
        {
            if (_initialized || _failed) return;
            lock (_lock)
            {
                if (_initialized || _failed) return;
                try
                {
                    _enabled = ReadEnabledPref();
                    _mainThreadId = Thread.CurrentThread.ManagedThreadId;
                    _processId = System.Diagnostics.Process.GetCurrentProcess().Id;
                    string projectRoot = Path.GetDirectoryName(Application.dataPath);
                    string dir = Path.Combine(projectRoot ?? ".", "Library", "MCPForUnity", "Logs");
                    Directory.CreateDirectory(dir);
                    _logPath = Path.Combine(dir, "diagnostic.log");
                    _initialized = true;
                    if (_enabled)
                    {
                        WriteLineUnsafe(FormatLine(
                            Level.Info,
                            "Boot",
                            $"diagnostic log opened pid={_processId} unity={Application.unityVersion} mainThread={_mainThreadId}"));
                    }
                }
                catch (Exception ex)
                {
                    _failed = true;
                    try { Debug.LogWarning("[McpDiagnosticLog] init failed: " + ex.Message); } catch { }
                }
            }
        }

        public static void SetEnabled(bool enabled)
        {
            try { EditorPrefs.SetBool(EditorPrefKeys.DiagnosticLogEnabled, enabled); } catch { }
            bool wasEnabled = _enabled;
            _enabled = enabled;
            if (enabled && !wasEnabled)
            {
                Event(Level.Info, "Config", "diagnostic logging enabled");
            }
            else if (!enabled && wasEnabled)
            {
                // Emit one final line directly so the toggle action is recorded even if IsEnabled now returns false.
                try
                {
                    lock (_lock)
                    {
                        if (_initialized && !_failed)
                        {
                            RotateIfNeededUnsafe();
                            WriteLineUnsafe(FormatLine(Level.Info, "Config", "diagnostic logging disabled"));
                        }
                    }
                }
                catch { }
            }
        }

        public static void Trace(string category, string message) => Event(Level.Trace, category, message);
        public static void Info(string category, string message) => Event(Level.Info, category, message);
        public static void Warn(string category, string message) => Event(Level.Warn, category, message);
        public static void Error(string category, string message) => Event(Level.Error, category, message);

        public static void Exception(string category, string message, Exception ex)
        {
            string detail = ex == null
                ? message
                : $"{message} {ex.GetType().Name}: {ex.Message}";
            Event(Level.Error, category, detail);
        }

        public static void Event(Level level, string category, string message)
        {
            if (!_initialized && !_failed)
            {
                // Lazy init: fine when called from main thread; if first call comes from a background
                // thread before any [InitializeOnLoad] hook has run, Init may fail accessing
                // Application.dataPath — in that case we mark _failed and skip silently.
                Init();
            }
            if (!IsEnabled) return;
            try
            {
                string line = FormatLine(level, category, message);
                lock (_lock)
                {
                    if (!_initialized || _failed) return;
                    RotateIfNeededUnsafe();
                    WriteLineUnsafe(line);
                }
            }
            catch
            {
                // never throw from logger
            }
        }

        private static bool ReadEnabledPref()
        {
            try { return EditorPrefs.GetBool(EditorPrefKeys.DiagnosticLogEnabled, true); }
            catch { return true; }
        }

        private static string FormatLine(Level level, string category, string message)
        {
            string ts = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
            int tid = Thread.CurrentThread.ManagedThreadId;
            string threadTag = tid == _mainThreadId ? "main" : ("T" + tid.ToString(CultureInfo.InvariantCulture));
            string lvl;
            switch (level)
            {
                case Level.Trace: lvl = "TRACE"; break;
                case Level.Info: lvl = "INFO "; break;
                case Level.Warn: lvl = "WARN "; break;
                case Level.Error: lvl = "ERROR"; break;
                default: lvl = "INFO "; break;
            }
            string msg = message ?? string.Empty;
            if (msg.Length > MaxMessageChars)
            {
                msg = msg.Substring(0, MaxMessageChars) + "...(truncated " + (msg.Length - MaxMessageChars) + " chars)";
            }
            return ts + "Z [" + threadTag + "] [" + lvl + "] [" + (category ?? "-") + "] " + msg;
        }

        private static void WriteLineUnsafe(string line)
        {
            using (var fs = new FileStream(_logPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete))
            using (var sw = new StreamWriter(fs, new UTF8Encoding(false)))
            {
                sw.WriteLine(line);
            }
        }

        private static void RotateIfNeededUnsafe()
        {
            try
            {
                var fi = new FileInfo(_logPath);
                if (!fi.Exists || fi.Length < MaxBytes) return;
                string backup = _logPath + ".1";
                try { if (File.Exists(backup)) File.Delete(backup); } catch { }
                try { File.Move(_logPath, backup); } catch { }
            }
            catch
            {
                // ignore — best-effort rotation
            }
        }
    }
}
