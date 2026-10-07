import { create } from 'zustand';

export type MemorySource = 'AUTO' | 'USER' | 'TOOL';
export interface MemoryEntry {
    id?: string;
    source: MemorySource;
    category: string;
    title?: string;
    timestamp: string;
    content: string;
    [key: string]: unknown;
}
export interface MemoryDocument {
    content: string;
    entries: MemoryEntry[];
    revision: number;
    updatedAt: string | null;
    maxSize: number;
}
interface MemoryStore extends MemoryDocument {
    scope: 'global' | 'project';
    projectPath: string;
    size: number;
    loading: boolean;
    saving: boolean;
    loaded: boolean;
    error: string | null;
    conflict: boolean;
    dirty: boolean;
    loadFile: () => Promise<void>;
    saveRaw: (content: string) => Promise<boolean>;
    saveEntries: (entries: MemoryEntry[]) => Promise<boolean>;
    setDirty: (dirty: boolean) => void;
    setScope: (scope: 'global' | 'project', projectPath?: string) => Promise<void>;
}
const byteSize = (content: string) => new TextEncoder().encode(content).length;
function parseDocument(value: unknown): MemoryDocument {
    const document = value as Partial<MemoryDocument> | null;
    if (!document || typeof document.content !== 'string' || !Array.isArray(document.entries)
            || typeof document.revision !== 'number' || !Number.isSafeInteger(document.revision)) {
        throw new Error('记忆响应格式无效');
    }
    return {
        content: document.content, entries: document.entries.map(entry => ({ ...entry,
            timestamp: entry.timestamp || entry.updatedAt || entry.createdAt || '',
        } as MemoryEntry)), revision: document.revision,
        updatedAt: document.updatedAt ?? null, maxSize: document.maxSize ?? 0,
    };
}

export const useMemoryStore = create<MemoryStore>((set, get) => {
    // Scope changes and reloads invalidate every earlier asynchronous response.
    let generation = 0;
    const context = () => ({
        scope: get().scope,
        projectPath: get().scope === 'project' ? get().projectPath : undefined,
    });
    const save = async (suffix: string, change: object): Promise<boolean> => {
        if (get().saving || get().loading || !get().loaded || get().conflict) return false;
        const request = ++generation;
        const body = { ...context(), expectedRevision: get().revision, ...change };
        set({ saving: true, error: null });
        try {
            const response = await fetch(`/api/memory/document${suffix}`, {
                method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
            });
            if (request !== generation) return false;
            if (response.status === 409) {
                set({ conflict: true, saving: false });
                return false;
            }
            if (!response.ok) throw new Error(`保存记忆失败（HTTP ${response.status}）`);
            const document = parseDocument(await response.json());
            if (request !== generation) return false;
            set({ ...document, size: byteSize(document.content), saving: false, dirty: false });
            return true;
        } catch (error) {
            if (request === generation) set({ saving: false, error: error instanceof Error ? error.message : '保存记忆失败' });
            return false;
        }
    };
    return {
        scope: 'global', projectPath: '', content: '', entries: [], revision: 0, updatedAt: null,
        maxSize: 0, size: 0, loading: false, saving: false, loaded: false, error: null,
        conflict: false, dirty: false,
        setDirty: dirty => set({ dirty }),
        setScope: async (scope, projectPath = '') => {
            generation++;
            set({ scope, projectPath, content: '', entries: [], revision: 0, loaded: false, dirty: false, saving: false });
            await get().loadFile();
        },
        loadFile: async () => {
            const request = ++generation;
            set({ loading: true, saving: false, error: null });
            try {
                const selected = context();
                if (selected.scope === 'project' && !selected.projectPath) throw new Error('请选择项目');
                const query = new URLSearchParams({ scope: selected.scope,
                    ...(selected.projectPath ? { projectPath: selected.projectPath } : {}) });
                const response = await fetch(`/api/memory/document?${query}`, { cache: 'no-store' });
                if (!response.ok) throw new Error(`读取记忆失败（HTTP ${response.status}）`);
                const document = parseDocument(await response.json());
                if (request === generation) set({ ...document, size: byteSize(document.content),
                    loading: false, loaded: true, conflict: false, dirty: false });
            } catch (error) {
                if (request === generation) set({ loading: false, loaded: false,
                    error: error instanceof Error ? error.message : '读取记忆失败' });
            }
        },
        saveRaw: content => save('', { content }),
        saveEntries: entries => save('/entries', { entries: entries.map(entry => ({
            id: entry.id, category: entry.category, title: entry.title ?? entry.content.trim().split('\n')[0].slice(0, 120),
            content: entry.content, keywords: entry.keywords ?? null, source: entry.source,
        })) }),
    };
});
