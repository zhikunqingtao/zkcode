/** Session-local view preferences over real tool records; hints never supply execution data. */
import { create } from 'zustand';
import { useSessionStore } from './sessionStore';
import { useMessageStore } from './messageStore';
import { extractToolCalls } from '@/utils/sequence-diagram-builder';

interface SequenceViewState {
    sessionId: string | null;
    tools: string[];
    selectedId: string | null;
    note: string | null;
    applyVisualizationHint: (props: Record<string, unknown>) => void;
    setTools: (tools: string[]) => void;
    selectRecord: (id: string | null) => void;
    reset: () => void;
}
const empty = () => ({ sessionId: useSessionStore.getState().sessionId, tools: [], selectedId: null, note: null });
const text = (value: unknown): string | null => typeof value === 'string' && value.trim().length > 0 && value.length <= 256 ? value.trim() : null;

export const useSequenceViewStore = create<SequenceViewState>()((set, get) => ({
    ...empty(),
    applyVisualizationHint: props => {
        const sessionId = useSessionStore.getState().sessionId;
        if (text(props.sessionId) && props.sessionId !== sessionId) {
            set({ ...empty(), note: '建议指向其他会话；只展示当前会话的实际调用。' });
            return;
        }
        const records = extractToolCalls(useMessageStore.getState().messages);
        const names = new Set(records.map(record => record.toolName));
        const candidates = Array.isArray(props.toolNames) ? props.toolNames.slice(0, 32) : [props.toolName];
        const requested = [...new Set(candidates.map(text).filter((name): name is string => name !== null))];
        const tools = requested.filter(name => names.has(name));
        const id = text(props.toolUseId ?? props.tool_use_id);
        const record = id ? records.find(item => item.toolUseId === id) : undefined;
        set({ sessionId, tools: record ? [record.toolName] : tools, selectedId: record?.toolUseId ?? null,
            note: record || tools.length ? '已定位当前会话的实际调用；建议未执行任何工具。'
                : '建议没有匹配当前会话的已有调用；可查看下方真实记录，不会生成或执行调用。' });
    },
    setTools: tools => set({ sessionId: useSessionStore.getState().sessionId, tools, selectedId: null, note: null }),
    selectRecord: id => set({ sessionId: useSessionStore.getState().sessionId, selectedId: id === get().selectedId ? null : id }),
    reset: () => set(empty()),
}));
const unsubscribe = useSessionStore.subscribe((state, previous) => {
    if (state.sessionId !== previous.sessionId) useSequenceViewStore.getState().reset();
});
if (import.meta.hot) import.meta.hot.dispose(unsubscribe);
