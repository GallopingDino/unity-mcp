using MCPForUnity.Editor.Constants;
using UnityEditor;
using UnityEngine;

namespace MCPForUnity.Editor.Services
{
    /// <summary>
    /// Reads and writes the <see cref="AutoStartPolicy"/> with one-shot lazy
    /// migration from the legacy boolean <c>MCPForUnity.AutoStartOnLoad</c> key.
    /// All package code that needs to read or update the policy must go through here.
    /// </summary>
    internal static class AutoStartPolicySettings
    {
        // SessionState resets when the Editor process restarts — the opt-out intentionally does
        // not survive a full Editor restart.
        private const string SessionEndedByUserKey = "MCPForUnity.SessionEndedByUser";

        /// <summary>
        /// On first call after upgrade from a legacy bool pref, migrates it
        /// (false → Disabled, true → OnEditorLoad) and deletes the legacy key.
        /// </summary>
        internal static AutoStartPolicy Get()
        {
            if (EditorPrefs.HasKey(EditorPrefKeys.AutoStartPolicy))
            {
                int raw = EditorPrefs.GetInt(EditorPrefKeys.AutoStartPolicy, (int)AutoStartPolicy.Disabled);
                return (AutoStartPolicy)Mathf.Clamp(raw, (int)AutoStartPolicy.Disabled, (int)AutoStartPolicy.KeepRunning);
            }

#pragma warning disable CS0618 // Legacy key is read here for migration only
            if (EditorPrefs.HasKey(EditorPrefKeys.AutoStartOnLoad))
            {
                bool legacy = EditorPrefs.GetBool(EditorPrefKeys.AutoStartOnLoad, false);
                AutoStartPolicy migrated = legacy ? AutoStartPolicy.OnEditorLoad : AutoStartPolicy.Disabled;
                Set(migrated);
                EditorPrefs.DeleteKey(EditorPrefKeys.AutoStartOnLoad);
                return migrated;
            }
#pragma warning restore CS0618

            return AutoStartPolicy.Disabled;
        }

        internal static void Set(AutoStartPolicy policy)
        {
            EditorPrefs.SetInt(EditorPrefKeys.AutoStartPolicy, (int)policy);
        }

        /// <summary>
        /// True if the user explicitly opted out of an active session in this Editor process.
        /// KeepRunning revive honors this until the user opts back in (Start Server / Start Session / policy change).
        /// </summary>
        internal static bool IsSessionEndedByUser()
        {
            return SessionState.GetBool(SessionEndedByUserKey, false);
        }

        internal static void SetSessionEndedByUser(bool value)
        {
            SessionState.SetBool(SessionEndedByUserKey, value);
        }
    }
}
