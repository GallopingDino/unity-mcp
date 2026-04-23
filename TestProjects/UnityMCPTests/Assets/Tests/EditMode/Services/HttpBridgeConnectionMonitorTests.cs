using NUnit.Framework;
using System;
using System.Threading;
using System.Threading.Tasks;
using MCPForUnity.Editor.Constants;
using MCPForUnity.Editor.Services;
using UnityEditor;

namespace MCPForUnityTests.Editor.Services
{
    [TestFixture]
    public class HttpBridgeConnectionMonitorTests
    {
        private const string PolicyKey = EditorPrefKeys.AutoStartPolicy;
        private const string UseHttpKey = EditorPrefKeys.UseHttpTransport;
        private const string ScopeKey = EditorPrefKeys.HttpTransportScope;

        private bool _hadPolicyKey;
        private int _originalPolicyValue;
        private bool _hadUseHttpKey;
        private bool _originalUseHttp;
        private bool _hadScopeKey;
        private string _originalScope;
        private bool _originalSessionEnded;
        private IServerManagementService _originalServer;
        private FakeServerService _fakeServer;

        [SetUp]
        public void SetUp()
        {
            _hadPolicyKey = EditorPrefs.HasKey(PolicyKey);
            _originalPolicyValue = EditorPrefs.GetInt(PolicyKey, 0);
            _hadUseHttpKey = EditorPrefs.HasKey(UseHttpKey);
            _originalUseHttp = EditorPrefs.GetBool(UseHttpKey, false);
            _hadScopeKey = EditorPrefs.HasKey(ScopeKey);
            _originalScope = EditorPrefs.GetString(ScopeKey, string.Empty);
            _originalSessionEnded = AutoStartPolicySettings.IsSessionEndedByUser();

            EditorPrefs.SetInt(PolicyKey, (int)AutoStartPolicy.KeepRunning);
            EditorPrefs.SetBool(UseHttpKey, true);
            EditorPrefs.SetString(ScopeKey, "local");
            AutoStartPolicySettings.SetSessionEndedByUser(false);
            EditorConfigurationCache.Instance.Refresh();

            _originalServer = MCPServiceLocator.Server;
            _fakeServer = new FakeServerService { Reachable = false, StartResult = true };
            MCPServiceLocator.Register<IServerManagementService>(_fakeServer);
        }

        [TearDown]
        public void TearDown()
        {
            if (_hadPolicyKey) EditorPrefs.SetInt(PolicyKey, _originalPolicyValue);
            else EditorPrefs.DeleteKey(PolicyKey);

            if (_hadUseHttpKey) EditorPrefs.SetBool(UseHttpKey, _originalUseHttp);
            else EditorPrefs.DeleteKey(UseHttpKey);

            if (_hadScopeKey) EditorPrefs.SetString(ScopeKey, _originalScope);
            else EditorPrefs.DeleteKey(ScopeKey);

            AutoStartPolicySettings.SetSessionEndedByUser(_originalSessionEnded);
            EditorConfigurationCache.Instance.Refresh();

            MCPServiceLocator.Register<IServerManagementService>(_originalServer);
        }

        [Test]
        public async Task Revive_PolicyDisabled_DoesNotStartServer()
        {
            EditorPrefs.SetInt(PolicyKey, (int)AutoStartPolicy.Disabled);
            await HttpBridgeConnectionMonitor.TryReviveServerAsync(CancellationToken.None);
            Assert.AreEqual(0, _fakeServer.StartCallCount);
        }

        [Test]
        public async Task Revive_PolicyOnEditorLoad_DoesNotStartServer()
        {
            EditorPrefs.SetInt(PolicyKey, (int)AutoStartPolicy.OnEditorLoad);
            await HttpBridgeConnectionMonitor.TryReviveServerAsync(CancellationToken.None);
            Assert.AreEqual(0, _fakeServer.StartCallCount);
        }

        [Test]
        public async Task Revive_SessionEndedByUser_DoesNotStartServer()
        {
            AutoStartPolicySettings.SetSessionEndedByUser(true);
            await HttpBridgeConnectionMonitor.TryReviveServerAsync(CancellationToken.None);
            Assert.AreEqual(0, _fakeServer.StartCallCount);
        }

        [Test]
        public async Task Revive_StdioTransport_DoesNotStartServer()
        {
            EditorPrefs.SetBool(UseHttpKey, false);
            EditorConfigurationCache.Instance.Refresh();
            await HttpBridgeConnectionMonitor.TryReviveServerAsync(CancellationToken.None);
            Assert.AreEqual(0, _fakeServer.StartCallCount);
        }

        [Test]
        public async Task Revive_RemoteScope_DoesNotStartServer()
        {
            EditorPrefs.SetString(ScopeKey, "remote");
            EditorConfigurationCache.Instance.Refresh();
            await HttpBridgeConnectionMonitor.TryReviveServerAsync(CancellationToken.None);
            Assert.AreEqual(0, _fakeServer.StartCallCount);
        }

        [Test]
        public async Task Revive_ServerAlreadyReachable_DoesNotStartServer()
        {
            _fakeServer.Reachable = true;
            await HttpBridgeConnectionMonitor.TryReviveServerAsync(CancellationToken.None);
            Assert.AreEqual(0, _fakeServer.StartCallCount);
        }

        [Test]
        public async Task Revive_AllGatesPass_CallsStartLocalHttpServer()
        {
            _fakeServer.Reachable = false;
            _fakeServer.StartResult = true;
            await HttpBridgeConnectionMonitor.TryReviveServerAsync(CancellationToken.None);
            Assert.AreEqual(1, _fakeServer.StartCallCount);
        }

        private sealed class FakeServerService : IServerManagementService
        {
            public bool Reachable { get; set; }
            public bool StartResult { get; set; } = true;
            public int StartCallCount { get; private set; }

            public bool IsLocalHttpServerReachable() => Reachable;

            public bool StartLocalHttpServer(bool quiet = false)
            {
                StartCallCount++;
                if (StartResult) Reachable = true;
                return StartResult;
            }

            public bool ClearUvxCache() => throw new NotImplementedException();
            public bool StopLocalHttpServer() => throw new NotImplementedException();
            public bool StopManagedLocalHttpServer() => throw new NotImplementedException();
            public bool IsLocalHttpServerRunning() => throw new NotImplementedException();
            public bool TryGetLocalHttpServerCommand(out string command, out string error) => throw new NotImplementedException();
            public bool IsLocalUrl() => throw new NotImplementedException();
            public bool CanStartLocalServer() => throw new NotImplementedException();
        }
    }
}
