/**
 * ApiKeysTab — LLM Provider API Keys 管理
 *
 * 加载 GET /api/llm-keys 展示 provider 列表，
 * 用户编辑后 PUT /api/llm-keys 仅提交修改过的 keys。
 */

import { useState, useEffect, useCallback, useId } from 'react';
import { useNotificationStore } from '@/store/notificationStore';
import { invalidateSpeechAvailability } from '@/store/speechAvailabilityStore';
import { useModelStore } from '@/store/modelStore';

interface ProviderEntry {
  name: string;
  label: string;
  has_key: boolean;
  masked_key: string | null;
}

interface ProvidersResponse {
  providers?: ProviderEntry[];
}

/** 本地行状态：区分"未触碰"、"已编辑"、"待清除" */
type RowState =
  | { kind: 'untouched' }
  | { kind: 'edited'; value: string }
  | { kind: 'cleared' };

export function ApiKeysTab() {
  const addNotification = useNotificationStore((s) => s.addNotification);

  const [providers, setProviders] = useState<ProviderEntry[]>([]);
  const [rowStates, setRowStates] = useState<Record<string, RowState>>({});
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);

  const loadProviders = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch('/api/llm-keys', { signal });
      if (!resp.ok) throw new Error(await responseErrorMessage(resp));
      const data = await parseProvidersResponse(resp);
      setProviders(data.providers ?? []);
      setRowStates({});
    } catch (caught) {
      if (caught instanceof DOMException && caught.name === 'AbortError') return;
      setError(errorMessage(caught));
    } finally {
      if (!signal?.aborted) setLoading(false);
    }
  }, []);

  /** 初始加载 */
  useEffect(() => {
    const controller = new AbortController();
    void loadProviders(controller.signal);
    return () => controller.abort();
  }, [loadProviders]);

  /** 当前有多少行被修改（含清除） */
  const dirtyCount = Object.values(rowStates).filter(
    (s) => s.kind !== 'untouched',
  ).length;

  /** 更新某一行的状态 */
  const setRow = useCallback((name: string, state: RowState) => {
    setRowStates((prev) => ({ ...prev, [name]: state }));
  }, []);

  /** 保存：仅提交修改过的 keys */
  const handleSave = useCallback(async () => {
    const keys: Record<string, string> = {};
    for (const [name, state] of Object.entries(rowStates)) {
      if (state.kind === 'edited') keys[name] = state.value;
      else if (state.kind === 'cleared') keys[name] = '';
    }
    if (Object.keys(keys).length === 0) return;

    setSaving(true);
    setSaveError(null);
    try {
      const resp = await fetch('/api/llm-keys', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ keys }),
      });
      if (!resp.ok) throw new Error(await responseErrorMessage(resp));
      const data = await parseProvidersResponse(resp);
      setProviders(data.providers ?? []);
      setRowStates({});
      await useModelStore.getState().fetchModels();
      if (Object.prototype.hasOwnProperty.call(keys, 'dashscope')) {
        void invalidateSpeechAvailability();
      }
      addNotification({
        key: 'apikeys-save-success',
        level: 'success',
        message: '密钥已更新，立即生效',
      });
    } catch (caught) {
      const message = errorMessage(caught);
      setSaveError(message);
      addNotification({
        key: 'apikeys-save-error',
        level: 'error',
        message: `保存失败：${message}`,
      });
    } finally {
      setSaving(false);
    }
  }, [rowStates, addNotification]);

  if (loading) {
    return (
      <div className="space-y-4">
        <h3 className="text-lg font-semibold text-[var(--text-primary)]">API Keys</h3>
        <p role="status" className="text-sm text-[var(--text-muted)]">加载中…</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="space-y-4">
        <h3 className="text-lg font-semibold text-[var(--text-primary)]">API Keys</h3>
        <p role="alert" className="text-sm text-red-500">加载失败：{error}</p>
        <button
          type="button"
          onClick={() => void loadProviders()}
          className="px-3 py-1.5 text-sm rounded bg-[var(--bg-secondary)] hover:bg-[var(--bg-hover)] text-[var(--text-primary)]"
        >
          重试
        </button>
      </div>
    );
  }

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <div>
          <h3 id="api-keys-heading" className="text-lg font-semibold text-[var(--text-primary)]">API Keys</h3>
          <p id="api-keys-description" className="text-sm text-[var(--text-muted)] mt-0.5">
            替换内置测试密钥或配置自己的 LLM Provider 密钥；保存后立即生效。
          </p>
        </div>
        <button
          type="button"
          onClick={handleSave}
          disabled={saving || dirtyCount === 0}
          aria-describedby="api-keys-description"
          className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors
            ${dirtyCount > 0 && !saving
              ? 'bg-blue-600 text-white hover:bg-blue-700'
              : 'bg-gray-100 dark:bg-gray-800 text-gray-400 dark:text-gray-500 cursor-not-allowed'
            }`}
        >
          {saving ? '保存中…' : '保存'}
        </button>
      </div>

      <div className="space-y-3" aria-labelledby="api-keys-heading">
        {providers.map((p) => (
          <ProviderRow
            key={p.name}
            provider={p}
            rowState={rowStates[p.name] ?? { kind: 'untouched' }}
            onChange={(state) => setRow(p.name, state)}
            disabled={saving}
          />
        ))}
        {providers.length === 0 && (
          <p className="text-sm text-[var(--text-muted)]">服务端未返回可配置的 Provider。</p>
        )}
      </div>

      {dirtyCount > 0 && (
        <p role="status" aria-live="polite" className="text-xs text-amber-600 dark:text-amber-400">
          有 {dirtyCount} 项未保存的更改
        </p>
      )}
      {saveError && (
        <p role="alert" className="text-sm text-red-500">
          保存失败：{saveError}
        </p>
      )}
    </div>
  );
}

/* ────────────────────────────────────────────────────────── */

interface ProviderRowProps {
  provider: ProviderEntry;
  rowState: RowState;
  onChange: (state: RowState) => void;
  disabled: boolean;
}

function ProviderRow({ provider, rowState, onChange, disabled }: ProviderRowProps) {
  const [focused, setFocused] = useState(false);
  const inputId = useId();
  const statusId = `${inputId}-status`;

  /** 当前输入框的值 */
  const inputValue =
    rowState.kind === 'edited' ? rowState.value
    : rowState.kind === 'cleared' ? ''
    : '';

  /** 是否已被标记为清除 */
  const isCleared = rowState.kind === 'cleared';

  /** 配置状态标签 */
  const statusLabel =
    isCleared ? '将清除已保存密钥'
    : rowState.kind === 'edited' ? '已修改'
    : provider.has_key ? '已配置'
    : '未配置';

  const statusColor =
    isCleared ? 'text-amber-600 dark:text-amber-400'
    : rowState.kind === 'edited' ? 'text-blue-600 dark:text-blue-400'
    : provider.has_key ? 'text-green-600 dark:text-green-400'
    : 'text-gray-400 dark:text-gray-500';

  const handleInput = (val: string) => {
    if (val === '') {
      // 若清空输入框则视为 untouched（用户可直接删除输入恢复到原始状态）
      onChange({ kind: 'untouched' });
    } else {
      onChange({ kind: 'edited', value: val });
    }
  };

  const handleClear = () => {
    onChange({ kind: 'cleared' });
  };

  const handleUndo = () => {
    onChange({ kind: 'untouched' });
  };

  const placeholder =
    focused ? '输入新密钥…'
    : provider.has_key && provider.masked_key ? provider.masked_key
    : provider.has_key ? '已配置'
    : '未配置（点击输入）';

  return (
    <div
      className={`flex flex-col sm:flex-row sm:items-center gap-3 px-4 py-3 rounded-lg border transition-colors
        ${isCleared || rowState.kind === 'edited'
          ? 'border-blue-300 dark:border-blue-700 bg-blue-50/40 dark:bg-blue-900/10'
          : 'border-gray-200 dark:border-gray-700 hover:border-gray-300 dark:hover:border-gray-600'
        }`}
    >
      {/* Label */}
      <div className="sm:w-44 shrink-0">
        <label htmlFor={inputId} className="text-sm font-medium text-[var(--text-primary)]">
          {provider.label}
        </label>
        <span id={statusId} aria-live="polite" className={`block text-xs mt-0.5 ${statusColor}`}>
          {statusLabel}
        </span>
      </div>

      {/* Input */}
      <input
        id={inputId}
        type="password"
        aria-describedby={statusId}
        aria-label={`${provider.label} API 密钥`}
        autoComplete="off"
        spellCheck={false}
        disabled={disabled}
        value={inputValue}
        placeholder={placeholder}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        onChange={(e) => handleInput(e.target.value)}
        className="flex-1 min-w-0 px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600
          bg-[var(--bg-secondary)] text-[var(--text-primary)] placeholder-[var(--text-muted)]
          focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
      />

      {/* Actions */}
      <div className="flex items-center gap-2 shrink-0">
        {provider.has_key && rowState.kind === 'untouched' && (
          <button
            type="button"
            onClick={handleClear}
            disabled={disabled}
            aria-label={`清除 ${provider.label} API 密钥`}
            title="清除当前保存的密钥；如果配置了环境变量，则恢复使用环境变量"
            className="px-2.5 py-1 text-xs rounded border border-gray-300 dark:border-gray-600
              text-gray-500 dark:text-gray-400
              hover:border-red-400 hover:text-red-500 dark:hover:border-red-500 dark:hover:text-red-400
              transition-colors"
          >
            清除
          </button>
        )}
        {(rowState.kind === 'edited' || rowState.kind === 'cleared') && (
          <button
            type="button"
            onClick={handleUndo}
            disabled={disabled}
            aria-label={`撤销 ${provider.label} API 密钥更改`}
            title="撤销更改"
            className="px-2.5 py-1 text-xs rounded border border-gray-300 dark:border-gray-600
              text-gray-500 dark:text-gray-400
              hover:border-gray-400 hover:text-gray-700 dark:hover:text-gray-300
              transition-colors"
          >
            撤销
          </button>
        )}
      </div>
    </div>
  );
}

function errorMessage(caught: unknown): string {
  return caught instanceof Error ? caught.message : String(caught);
}

async function parseProvidersResponse(resp: Response): Promise<ProvidersResponse> {
  const data = await resp.json() as ProvidersResponse;
  if (data.providers !== undefined && !Array.isArray(data.providers)) {
    throw new Error('服务端返回了无效的 Provider 列表');
  }
  return data;
}

async function responseErrorMessage(resp: Response): Promise<string> {
  const fallback = `请求失败（HTTP ${resp.status}）`;
  try {
    const contentType = resp.headers.get('content-type') ?? '';
    if (!contentType.includes('application/json')) return fallback;
    const payload = await resp.json() as { code?: unknown; message?: unknown; requestId?: unknown };
    const code = typeof payload.code === 'string' ? payload.code : null;
    const message = typeof payload.message === 'string' ? payload.message : null;
    const requestId = typeof payload.requestId === 'string' ? payload.requestId : null;
    const detail = [code, message].filter(Boolean).join('：');
    return `${detail || fallback}${requestId ? `（请求 ID：${requestId}）` : ''}`;
  } catch {
    return fallback;
  }
}
