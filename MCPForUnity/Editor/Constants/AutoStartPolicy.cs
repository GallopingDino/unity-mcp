namespace MCPForUnity.Editor.Constants
{
    /// <summary>
    /// Controls automatic startup of the local MCP HTTP server.
    /// Persisted as an int under <see cref="EditorPrefKeys.AutoStartPolicy"/>.
    /// </summary>
    internal enum AutoStartPolicy
    {
        Disabled = 0,
        OnEditorLoad = 1,
        KeepRunning = 2
    }
}
