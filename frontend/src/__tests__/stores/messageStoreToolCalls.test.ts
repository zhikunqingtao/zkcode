import { beforeEach, describe, expect, it } from 'vitest';
import { useMessageStore } from '@/store/messageStore';
import type { Message, Usage } from '@/types';
const usage: Usage = { inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 };
function tool(message: Message, id: string) { return message.type === 'assistant' ? message.content.find(b => b.type === 'tool_use' && b.toolUseId === id) : undefined; }
beforeEach(() => useMessageStore.getState().clearMessages());
describe('native tool lifetime across assistant segments', () => {
    it('retains input and result in the sealed message and keeps the terminal replay guard', () => {
        const store = useMessageStore.getState();
        store.appendStreamDelta('work'); store.startToolCall('read', 'Read', {});
        store.updateToolCallInput('read', { path: 'a.ts' }); store.completeToolCall('read', { content: 'ok', isError: false });
        store.finalizeStream(usage); store.startToolCall('read', 'Read', {});
        const state = useMessageStore.getState();
        expect(tool(state.messages[0], 'read')).toMatchObject({ input: { path: 'a.ts' }, result: { content: 'ok', isError: false } });
        expect(state.activeToolCalls.get('read')?.status).toBe('completed');
        expect(state.streamingMessageId).toBeNull();
    });
    it('a late result patches a previously sealed tool without opening another message', () => {
        const store = useMessageStore.getState(); store.startToolCall('read', 'Read', { path: 'a' }); store.finalizeAssistantSegment();
        store.completeToolCall('read', { content: 'late', isError: false });
        const state = useMessageStore.getState(); expect(state.messages).toHaveLength(1);
        expect(tool(state.messages[0], 'read')).toMatchObject({ result: { content: 'late' } });
    });
    it('an error seals unfinished root tools without changing child partitions or completed results', () => {
        const store = useMessageStore.getState(); store.startToolCall('root', 'Read', {}); store.startToolCall('child', 'Read', {}, 'child');
        store.startToolCall('done', 'Read', {}); store.completeToolCall('done', { content: 'real result', isError: false });
        store.finalizeAssistantSegment(); store.failAllRunningToolCalls('run failed');
        const state = useMessageStore.getState();
        expect(state.activeToolCalls.get('root')?.status).toBe('error');
        expect(state.activeToolCalls.get('child\0child')?.status).toBe('preparing');
        expect(state.activeToolCalls.get('done')?.result?.content).toBe('real result');
        expect(tool(state.messages[0], 'root')).toMatchObject({ result: { content: 'run failed', isError: true } });
    });
    it('authoritative committed results guard against delayed duplicate starts after reconciliation', () => {
        const result: Message = { type: 'assistant', uuid: 'committed', timestamp: 1, usage, stopReason: 'end_turn', content: [{ type: 'tool_use', toolUseId: 'read', toolName: 'Read', input: {}, result: { content: 'real', isError: false } }] };
        const store = useMessageStore.getState(); expect(store.reconcileCommittedRun(null, [result])).toBe(true);
        store.startToolCall('read', 'Read', {}); expect(useMessageStore.getState().messages).toEqual([result]);
        expect(useMessageStore.getState().activeToolCalls.has('read')).toBe(false);
    });
    it('unknown input and result frames remain recoverable instead of being discarded', () => {
        const store = useMessageStore.getState(); store.updateToolCallInput('recovered', { a: 1 });
        store.completeToolCall('recovered', { content: 'ok', isError: false });
        expect(useMessageStore.getState().activeToolCalls.get('recovered')).toMatchObject({ input: { a: 1 }, status: 'completed', result: { content: 'ok' } });
    });
});
