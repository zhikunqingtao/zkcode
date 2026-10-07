import { create } from 'zustand';
import type { Message } from '@/types';

export interface ToolPresentation {
    runId: string;
    toolUseId: string;
    assistantMessageId: string | null;
    text: string;
    sequence: number;
}
interface State {
    sessionId: string | null;
    entries: Record<string, ToolPresentation>;
    loaded: boolean;
    loading: boolean;
    error: string | null;
    revision: number;
    activate: (sessionId: string | null, refresh?: boolean) => void;
    record: (sessionId: string, runId: string, toolUseId: string, text: string) => void;
    bindCommitted: (sessionId: string, runId: string, messages: Message[]) => void;
    load: (sessionId: string) => Promise<void>;
}
const key = (run: string, tool: string) => JSON.stringify([run, tool]);
const MAX_ENTRIES = 2000;
const MAX_TEXT = 8 * 1024 * 1024;
let controller: AbortController | null = null;
let generation = 0;
function valid(value: unknown): value is ToolPresentation {
    if (!value || typeof value !== 'object') return false;
    const item = value as ToolPresentation;
    return typeof item.runId === 'string' && !!item.runId && typeof item.toolUseId === 'string' && !!item.toolUseId
        && (item.assistantMessageId === null || typeof item.assistantMessageId === 'string')
        && typeof item.text === 'string' && item.text.length <= 32768
        && Number.isSafeInteger(item.sequence) && item.sequence >= 0;
}
function bounded(entries: Record<string, ToolPresentation>): boolean {
    const values = Object.values(entries);
    return values.length <= MAX_ENTRIES && values.reduce((total, item) => total + item.text.length, 0) <= MAX_TEXT;
}

/** UI-only projection. Never persisted in browser storage or copied into tool facts. */
export const useToolPresentationStore = create<State>((set, get) => ({
    sessionId: null, entries: {}, loaded: false, loading: false, error: null, revision: 0,
    activate: (sessionId, refresh = false) => {
        if (get().sessionId === sessionId && !refresh) return;
        generation += 1; controller?.abort(); controller = null;
        set({ sessionId, entries: get().sessionId === sessionId ? get().entries : {}, loaded: false, loading: false, error: null, revision: get().revision + 1 });
    },
    record: (sessionId, runId, toolUseId, text) => {
        get().activate(sessionId);
        const id = key(runId, toolUseId);
        const entries = { ...get().entries, [id]: { runId, toolUseId, assistantMessageId: get().entries[id]?.assistantMessageId ?? null, text: text.slice(0, 32768), sequence: get().entries[id]?.sequence ?? 0 } };
        if (bounded(entries)) set({ entries });
        else set({ error: 'Hook 展示备注超出本页内存上限；原始工具结果仍然完整。' });
    },
    bindCommitted: (sessionId, runId, messages) => {
        if (get().sessionId !== sessionId) return;
        const entries = { ...get().entries };
        for (const message of messages) {
            if (message.type !== 'assistant') continue;
            for (const block of message.content) {
                if (block.type !== 'tool_use') continue;
                const id = key(runId, block.toolUseId);
                if (entries[id]) entries[id] = { ...entries[id], assistantMessageId: message.uuid };
            }
        }
        set({ entries });
    },
    load: async sessionId => {
        get().activate(sessionId);
        if (get().loaded || get().loading) return;
        const token = generation;
        controller = new AbortController();
        const signal = controller.signal;
        set({ loading: true, error: null });
        try {
            let after = 0;
            for (let page = 0; page < 100; page += 1) {
                const response = await fetch(`/api/sessions/${encodeURIComponent(sessionId)}/tool-presentations?after=${after}`, { headers: { 'X-Session-Id': sessionId }, signal });
                if (!response.ok) throw new Error(`Hook 展示备注恢复失败（HTTP ${response.status}）；原始工具结果仍然可见。`);
                const body: unknown = await response.json();
                if (token !== generation || get().sessionId !== sessionId) return;
                const result = body as { presentations?: unknown[]; nextCursor?: number | null };
                if (!result || !Array.isArray(result.presentations) || result.presentations.length > 200 || !result.presentations.every(valid)) throw new Error('Hook 展示备注响应无效');
                const entries = { ...get().entries };
                for (const item of result.presentations as ToolPresentation[]) {
                    const id = key(item.runId, item.toolUseId);
                    if (!entries[id] || entries[id].sequence <= item.sequence) entries[id] = { ...item, assistantMessageId: item.assistantMessageId ?? entries[id]?.assistantMessageId ?? null };
                }
                if (!bounded(entries)) throw new Error('Hook 展示备注超出本页内存上限；已加载备注与原始工具结果仍然可见。');
                set({ entries });
                if (result.nextCursor === null) { set({ loaded: true }); return; }
                if (!Number.isSafeInteger(result.nextCursor) || result.nextCursor! <= after) throw new Error('Hook 展示备注恢复游标无效');
                after = result.nextCursor!;
            }
            throw new Error('Hook 展示备注页数超出上限');
        } catch (error) {
            if (token === generation && !signal.aborted) set({ error: error instanceof Error ? error.message : 'Hook 展示备注恢复失败' });
        } finally {
            if (token === generation) set({ loading: false });
        }
    },
}));

export function selectToolPresentation(state: State, sessionId: string | null, toolUseId: string, runId?: string, messageId?: string): string {
    if (!sessionId || state.sessionId !== sessionId) return '';
    if (messageId) {
        const matches = Object.values(state.entries).filter(item => item.assistantMessageId === messageId && item.toolUseId === toolUseId);
        if (matches.length === 1) return matches[0].text;
        if (matches.length === 0 && runId) {
            const live = state.entries[key(runId, toolUseId)];
            return live?.assistantMessageId === null ? live.text : '';
        }
        return '';
    }
    return runId ? state.entries[key(runId, toolUseId)]?.text ?? '' : '';
}
