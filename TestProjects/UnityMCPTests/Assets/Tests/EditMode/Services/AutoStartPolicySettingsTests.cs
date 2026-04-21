using NUnit.Framework;
using MCPForUnity.Editor.Constants;
using MCPForUnity.Editor.Services;
using UnityEditor;

namespace MCPForUnityTests.Editor.Services
{
    /// <summary>
    /// Unit tests for AutoStartPolicySettings — covers default, persistence,
    /// and one-shot migration from the legacy bool key.
    /// </summary>
    [TestFixture]
    public class AutoStartPolicySettingsTests
    {
        // We deliberately reference the legacy key to verify migration.
        // Suppress the obsolete warning at the test fixture level.
#pragma warning disable CS0618
        private const string LegacyKey = EditorPrefKeys.AutoStartOnLoad;
#pragma warning restore CS0618
        private const string NewKey = EditorPrefKeys.AutoStartPolicy;

        private bool _hadNewKey;
        private int _originalNewValue;
        private bool _hadLegacyKey;
        private bool _originalLegacyValue;

        [SetUp]
        public void SetUp()
        {
            // Save original values
            _hadNewKey = EditorPrefs.HasKey(NewKey);
            _originalNewValue = EditorPrefs.GetInt(NewKey, 0);
            _hadLegacyKey = EditorPrefs.HasKey(LegacyKey);
            _originalLegacyValue = EditorPrefs.GetBool(LegacyKey, false);

            EditorPrefs.DeleteKey(NewKey);
            EditorPrefs.DeleteKey(LegacyKey);
        }

        [TearDown]
        public void TearDown()
        {
            // Restore original values
            if (_hadNewKey)
                EditorPrefs.SetInt(NewKey, _originalNewValue);
            else
                EditorPrefs.DeleteKey(NewKey);

            if (_hadLegacyKey)
                EditorPrefs.SetBool(LegacyKey, _originalLegacyValue);
            else
                EditorPrefs.DeleteKey(LegacyKey);
        }

        #region Get

        [Test]
        public void Get_NoKeys_ReturnsDisabled()
        {
            // Act
            var policy = AutoStartPolicySettings.Get();

            // Assert
            Assert.AreEqual(AutoStartPolicy.Disabled, policy, "Fresh install must default to Disabled");
        }

        [Test]
        public void Get_NewKeyPresent_IgnoresLegacy()
        {
            // Arrange — both keys present, legacy says OnEditorLoad, new says KeepRunning
            EditorPrefs.SetInt(NewKey, (int)AutoStartPolicy.KeepRunning);
            EditorPrefs.SetBool(LegacyKey, true);

            // Act
            var policy = AutoStartPolicySettings.Get();

            // Assert
            Assert.AreEqual(AutoStartPolicy.KeepRunning, policy, "New key must win when both are present");
            Assert.IsTrue(EditorPrefs.HasKey(LegacyKey), "Legacy key must be left alone — no double-migration");
        }

        #endregion

        #region Set

        [Test]
        public void Set_PersistsAsInt()
        {
            // Act
            AutoStartPolicySettings.Set(AutoStartPolicy.KeepRunning);

            // Assert
            Assert.AreEqual((int)AutoStartPolicy.KeepRunning, EditorPrefs.GetInt(NewKey, -1));
        }

        #endregion

        #region Migration

        [Test]
        public void Get_LegacyBoolFalse_MigratesToDisabled()
        {
            // Arrange
            EditorPrefs.SetBool(LegacyKey, false);

            // Act
            var policy = AutoStartPolicySettings.Get();

            // Assert
            Assert.AreEqual(AutoStartPolicy.Disabled, policy);
            Assert.AreEqual((int)AutoStartPolicy.Disabled, EditorPrefs.GetInt(NewKey, -1),
                "Migration must persist the converted value to the new key");
        }

        [Test]
        public void Get_LegacyBoolTrue_MigratesToOnEditorLoad()
        {
            // Arrange
            EditorPrefs.SetBool(LegacyKey, true);

            // Act
            var policy = AutoStartPolicySettings.Get();

            // Assert
            Assert.AreEqual(AutoStartPolicy.OnEditorLoad, policy);
            Assert.AreEqual((int)AutoStartPolicy.OnEditorLoad, EditorPrefs.GetInt(NewKey, -1),
                "Migration must persist the converted value to the new key");
        }

        [Test]
        public void Get_AfterMigration_LegacyKeyDeleted()
        {
            // Arrange
            EditorPrefs.SetBool(LegacyKey, true);

            // Act
            AutoStartPolicySettings.Get();

            // Assert
            Assert.IsFalse(EditorPrefs.HasKey(LegacyKey), "Legacy key must be removed after migration");
        }

        #endregion
    }
}
