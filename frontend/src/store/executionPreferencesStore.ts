import { create } from 'zustand';
export interface ExecutionPreferences {
    revision: number; effort: string; fast: boolean; effectiveModel: string; fastAvailable: boolean; supportedEfforts: string[]; validationError?: string;
}
interface Entry { value?: ExecutionPreferences; loading: boolean; saving: boolean; error: string | null }
interface ExecutionPreferencesState {
    sessions: Record<string, Entry>;
    load: (sessionId: string) => Promise<void>;
    update: (sessionId: string, patch: { effort?: string; fast?: boolean }) => Promise<boolean>;
}
const generations = new Map<string, number>();
const validate = (value: unknown): ExecutionPreferences => {
    const item = value as ExecutionPreferences;
    if (!item || !Number.isSafeInteger(item.revision) || item.revision < 0 || typeof item.fast !== 'boolean' || typeof item.fastAvailable !== 'boolean' || typeof item.effectiveModel !== 'string' || !['auto', 'low', 'medium', 'high', 'xhigh', 'max'].includes(item.effort) || !Array.isArray(item.supportedEfforts) || item.supportedEfforts.some(effort => !['low', 'medium', 'high', 'xhigh', 'max'].includes(effort))) throw new Error('会话执行配置响应无效');
    return item;
};
export const useExecutionPreferencesStore = create<ExecutionPreferencesState>((set, get) => {
    const merge = (id: string, patch: Partial<Entry>) => set(state => ({ sessions: { ...state.sessions, [id]: { ...(state.sessions[id] ?? { loading: false, saving: false, error: null }), ...patch } } }));
    return {
        sessions: {},
        load: async id => {
            if (get().sessions[id]?.loading || get().sessions[id]?.saving) return;
            const generation = (generations.get(id) ?? 0) + 1; generations.set(id, generation); merge(id, { loading: true });
            try {
                const response = await fetch(`/api/sessions/${encodeURIComponent(id)}/execution-preferences`, { headers: { 'X-Session-Id': id } });
                if (!response.ok) throw new Error(`加载会话执行配置失败（HTTP ${response.status}）`);
                const value = validate(await response.json());
                if (generations.get(id) === generation) merge(id, { value, error: null });
            } catch (error) { if (generations.get(id) === generation) merge(id, { error: error instanceof Error ? error.message : '加载失败' }); }
            finally { if (generations.get(id) === generation) merge(id, { loading: false }); }
        },
        update: async (id, patch) => {
            const entry = get().sessions[id];
            if (!entry?.value || entry.saving || entry.loading) return false;
            const generation = (generations.get(id) ?? 0) + 1; generations.set(id, generation); merge(id, { saving: true, error: null });
            try {
                const response = await fetch(`/api/sessions/${encodeURIComponent(id)}/execution-preferences`, { method: 'PATCH', headers: { 'Content-Type': 'application/json', 'X-Session-Id': id }, body: JSON.stringify({ ...patch, revision: entry.value.revision }) });
                if (response.status === 409) {
                    merge(id, { saving: false });
                    await get().load(id);
                    merge(id, { error: '配置已在其他入口改变，已重新加载；请检查后重试。' });
                    return false;
                }
                if (!response.ok) { const error = await response.json().catch(() => ({})); throw new Error(error.message ?? `保存失败（HTTP ${response.status}）`); }
                const value = validate(await response.json());
                if (generations.get(id) === generation) merge(id, { value, error: null });
                return true;
            } catch (error) { if (generations.get(id) === generation) merge(id, { error: error instanceof Error ? error.message : '保存失败' }); return false; }
            finally { if (generations.get(id) === generation) merge(id, { saving: false }); }
        },
    };
});
