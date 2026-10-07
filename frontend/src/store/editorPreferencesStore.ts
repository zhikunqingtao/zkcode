import { create } from 'zustand';
import { DEFAULT_EDITOR_PREFERENCES, validateEditorPreferences, type EditorPreferences } from '@/keyboard/shortcuts';

interface EditorPreferencesState {
    preferences: EditorPreferences;
    loaded: boolean;
    loading: boolean;
    saving: boolean;
    error: string | null;
    load: () => Promise<void>;
    save: (preferences: EditorPreferences) => Promise<boolean>;
    accept: (preferences: unknown) => void;
}
let generation = 0;
export const useEditorPreferencesStore = create<EditorPreferencesState>((set, get) => ({
    preferences: DEFAULT_EDITOR_PREFERENCES, loaded: false, loading: false, saving: false, error: null,
    load: async () => {
        if (get().loading || get().saving) return;
        const request = ++generation;
        set({ loading: true });
        try {
            const response = await fetch('/api/config');
            if (!response.ok) throw new Error(`加载编辑器配置失败（HTTP ${response.status}）`);
            const value: unknown = await response.json();
            if (!value || typeof value !== 'object') throw new Error('编辑器配置响应无效');
            const configured = (value as { editorPreferences?: unknown }).editorPreferences;
            const preferences = configured === undefined ? DEFAULT_EDITOR_PREFERENCES : validateEditorPreferences(configured);
            if (generation === request) set({ preferences, loaded: true, error: null });
        } catch (error) {
            if (generation === request) set({ error: error instanceof Error ? error.message : '编辑器配置加载失败，保留最近有效设置' });
        } finally { if (generation === request) set({ loading: false }); }
    },
    save: async (input) => {
        if (get().saving) return false;
        const request = ++generation;
        set({ saving: true, loading: false, error: null });
        try {
            const preferences = validateEditorPreferences(input);
            const response = await fetch('/api/config', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ editorPreferences: preferences }) });
            if (!response.ok) throw new Error(`保存编辑器配置失败（HTTP ${response.status}）`);
            const result = await response.json();
            if (result.success !== true) throw new Error('编辑器配置未保存');
            const saved = validateEditorPreferences(result.config?.editorPreferences);
            if (generation === request) set({ preferences: saved, loaded: true, error: null });
            return true;
        } catch (error) {
            if (generation === request) set({ error: error instanceof Error ? error.message : '编辑器配置保存失败' });
            return false;
        } finally { if (generation === request) set({ saving: false }); }
    },
    accept: value => {
        try {
            const preferences = validateEditorPreferences(value);
            generation++;
            set({ preferences, loaded: true, loading: false, saving: false, error: null });
        } catch { set({ error: '服务端编辑器配置无效，保留最近有效设置' }); }
    },
}));
