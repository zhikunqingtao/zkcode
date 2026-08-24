/**
 * PluginManager — 插件管理面板
 *
 * 对接 zkcode 后端 REST 端点（Batch 8G Step 3）：
 *   GET    /api/plugins            → { plugins: PluginInfo[], count }
 *   POST   /api/plugins/install     { path } → { status, plugin }
 *   DELETE /api/plugins/{id}        → { status, pluginId }
 *   POST   /api/plugins/reload      → { status, count }
 */

import { useState, useEffect, useCallback } from 'react';

interface PluginInfo {
  name: string;
  version: string;
  author: string;
  description: string;
  source: string;
  isBuiltin: boolean;
  enabled: boolean;
  hooks: string[];
}

interface PluginListResponse {
  plugins: PluginInfo[];
  count: number;
}

export default function PluginManager() {
  const [plugins, setPlugins] = useState<PluginInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [reloading, setReloading] = useState(false);
  const [installPath, setInstallPath] = useState('');
  const [installing, setInstalling] = useState(false);
  const [uninstalling, setUninstalling] = useState<string | null>(null);

  const fetchPlugins = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const res = await fetch('/api/plugins');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data: PluginListResponse = await res.json();
      setPlugins(data.plugins || []);
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : 'Failed to load plugins';
      setError(message);
    } finally {
      setLoading(false);
    }
  }, []);

  const reloadPlugins = useCallback(async () => {
    try {
      setReloading(true);
      setError(null);
      const res = await fetch('/api/plugins/reload', { method: 'POST' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      await fetchPlugins();
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : 'Failed to reload plugins';
      setError(message);
    } finally {
      setReloading(false);
    }
  }, [fetchPlugins]);

  const installPlugin = useCallback(async () => {
    const path = installPath.trim();
    if (!path) return;
    try {
      setInstalling(true);
      setError(null);
      const res = await fetch('/api/plugins/install', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => null);
        throw new Error(body?.message || `HTTP ${res.status}`);
      }
      setInstallPath('');
      await fetchPlugins();
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : 'Failed to install plugin';
      setError(message);
    } finally {
      setInstalling(false);
    }
  }, [installPath, fetchPlugins]);

  const uninstallPlugin = useCallback(async (pluginId: string) => {
    try {
      setUninstalling(pluginId);
      setError(null);
      const res = await fetch(
        `/api/plugins/${encodeURIComponent(pluginId)}`,
        { method: 'DELETE' },
      );
      if (!res.ok) {
        const body = await res.json().catch(() => null);
        throw new Error(body?.message || `HTTP ${res.status}`);
      }
      await fetchPlugins();
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : 'Failed to uninstall plugin';
      setError(message);
    } finally {
      setUninstalling(null);
    }
  }, [fetchPlugins]);

  useEffect(() => {
    void fetchPlugins();
  }, [fetchPlugins]);

  if (loading) {
    return <div className="p-4 text-muted">Loading plugins...</div>;
  }

  return (
    <div className="p-4 space-y-4">
      {/* Header + Reload */}
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Plugin Manager</h2>
        <button
          onClick={() => void reloadPlugins()}
          disabled={reloading}
          className="px-3 py-1 text-sm bg-primary text-white rounded hover:bg-primary-hover disabled:opacity-50"
        >
          {reloading ? 'Reloading...' : 'Reload All'}
        </button>
      </div>

      {/* Error banner */}
      {error && (
        <div className="p-3 bg-danger/10 text-danger rounded border border-danger/20 text-sm">
          {error}
        </div>
      )}

      {/* Install form */}
      <div className="flex gap-2">
        <input
          type="text"
          value={installPath}
          onChange={(e) => setInstallPath(e.target.value)}
          placeholder="Plugin directory path (e.g. /path/to/my-plugin)"
          className="flex-1 px-3 py-1.5 text-sm border border-border rounded bg-surface focus:outline-none focus:border-primary"
          onKeyDown={(e) => {
            if (e.key === 'Enter') void installPlugin();
          }}
        />
        <button
          onClick={() => void installPlugin()}
          disabled={installing || !installPath.trim()}
          className="px-4 py-1.5 text-sm bg-primary text-white rounded hover:bg-primary-hover disabled:opacity-50 whitespace-nowrap"
        >
          {installing ? 'Installing...' : 'Install'}
        </button>
      </div>

      {/* Plugin list */}
      {plugins.length === 0 ? (
        <div className="text-muted text-center py-8 border border-dashed border-border rounded">
          No plugins loaded. Place plugin directories in <code className="px-1 bg-surface-sunken rounded">.zk/plugins/</code> or install by path above.
        </div>
      ) : (
        <div className="space-y-2">
          {plugins.map((plugin) => (
            <div
              key={plugin.name}
              className="border border-border rounded-lg p-3 hover:shadow-sm transition-shadow"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="font-medium">{plugin.name}</span>
                  <span className="text-xs text-muted">v{plugin.version}</span>
                  {plugin.isBuiltin && (
                    <span className="px-1.5 py-0.5 text-xs bg-surface-sunken text-muted rounded">
                      builtin
                    </span>
                  )}
                  {plugin.source && (
                    <span className="px-1.5 py-0.5 text-xs bg-surface-sunken text-muted rounded">
                      {plugin.source}
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-2">
                  <span
                    className={`text-xs px-2 py-0.5 rounded ${
                      plugin.enabled
                        ? 'bg-success/10 text-success'
                        : 'bg-surface-sunken text-muted'
                    }`}
                  >
                    {plugin.enabled ? 'Enabled' : 'Disabled'}
                  </span>
                  {!plugin.isBuiltin && (
                    <button
                      onClick={() => void uninstallPlugin(plugin.name)}
                      disabled={uninstalling === plugin.name}
                      className="px-2 py-0.5 text-xs text-danger border border-danger/30 rounded hover:bg-danger/10 disabled:opacity-50"
                    >
                      {uninstalling === plugin.name ? '...' : 'Uninstall'}
                    </button>
                  )}
                </div>
              </div>
              {plugin.description && (
                <p className="text-sm text-muted mt-1">{plugin.description}</p>
              )}
              <div className="flex flex-wrap gap-3 mt-1.5 text-xs text-muted">
                {plugin.author && <span>by {plugin.author}</span>}
                {plugin.hooks.length > 0 && (
                  <span>{plugin.hooks.length} hooks: {plugin.hooks.join(', ')}</span>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
