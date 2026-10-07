import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest';
import { selectToolPresentation, useToolPresentationStore } from './toolPresentationStore';
import type { Message } from '@/types';

const assistant = (id: string): Message => ({ type: 'assistant', uuid: id, timestamp: 1, stopReason: 'tool_use', usage: { inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 }, content: [{ type: 'tool_use', toolUseId: 'same-id', toolName: 'Bash', input: {}, result: { content: 'real failure', isError: true } }] });
const page = (rows: unknown[], nextCursor: number | null = null) => new Response(JSON.stringify({ presentations: rows, nextCursor }));

describe('independent Hook presentation ownership', () => {
    beforeEach(() => useToolPresentationStore.getState().activate(null));
    afterEach(() => vi.unstubAllGlobals());
    it('survives canonical replacement without modifying a tool fact and isolates reused tool IDs by message/run', () => {
        const store = useToolPresentationStore.getState();
        const first = assistant('m1');
        const original = JSON.stringify(first);
        store.record('s1', 'r1', 'same-id', 'first note');
        expect(selectToolPresentation(useToolPresentationStore.getState(), 's1', 'same-id', 'r1', 'temporary-live')).toBe('first note');
        store.bindCommitted('s1', 'r1', [first]);
        store.record('s1', 'r2', 'same-id', 'second note');
        store.bindCommitted('s1', 'r2', [assistant('m2')]);
        const state = useToolPresentationStore.getState();
        expect(selectToolPresentation(state, 's1', 'same-id', undefined, 'm1')).toBe('first note');
        expect(selectToolPresentation(state, 's1', 'same-id', undefined, 'm2')).toBe('second note');
        expect(selectToolPresentation(state, 's1', 'same-id')).toBe('');
        expect(selectToolPresentation(state, 's2', 'same-id', 'r1', 'm1')).toBe('');
        expect(JSON.stringify(first)).toBe(original);
    });
    it('restores paged notes with authorized session headers and refuses to guess an unbound historical tool', async () => {
        const fetch = vi.fn().mockResolvedValueOnce(page([{ runId: 'r1', toolUseId: 'same-id', assistantMessageId: 'm1', text: 'restored', sequence: 7 }], 7))
            .mockResolvedValueOnce(page([{ runId: 'r2', toolUseId: 'other', assistantMessageId: null, text: 'live only', sequence: 8 }]));
        vi.stubGlobal('fetch', fetch);
        await useToolPresentationStore.getState().load('s1');
        expect(fetch.mock.calls[0][1].headers).toEqual({ 'X-Session-Id': 's1' });
        expect(fetch.mock.calls[1][0]).toContain('after=7');
        const state = useToolPresentationStore.getState();
        expect(selectToolPresentation(state, 's1', 'same-id', undefined, 'm1')).toBe('restored');
        expect(selectToolPresentation(state, 's1', 'other', undefined, 'history')).toBe('');
        expect(state.loaded).toBe(true);
    });
    it('discards a late response after switching sessions', async () => {
        let resolve!: (value: Response) => void;
        vi.stubGlobal('fetch', vi.fn(() => new Promise<Response>(done => { resolve = done; })));
        const pending = useToolPresentationStore.getState().load('old');
        useToolPresentationStore.getState().activate('new');
        resolve(page([{ runId: 'r', toolUseId: 'same-id', assistantMessageId: 'm', text: 'private old note', sequence: 1 }]));
        await pending;
        expect(useToolPresentationStore.getState().sessionId).toBe('new');
        expect(useToolPresentationStore.getState().entries).toEqual({});
    });
    it('keeps a valid live projection on restore failure and exposes the error', async () => {
        useToolPresentationStore.getState().record('s', 'r', 'same-id', 'live');
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('', { status: 503 })));
        await useToolPresentationStore.getState().load('s');
        expect(selectToolPresentation(useToolPresentationStore.getState(), 's', 'same-id', 'r')).toBe('live');
        expect(useToolPresentationStore.getState().error).toContain('503');
    });
    it('reloads the same session after reconnect while rejecting the previous request generation', async () => {
        let resolveOld!: (value: Response) => void;
        const fetch = vi.fn().mockImplementationOnce(() => new Promise<Response>(resolve => { resolveOld = resolve; }))
            .mockResolvedValueOnce(page([{ runId: 'r2', toolUseId: 'same-id', assistantMessageId: 'm2', text: 'current', sequence: 2 }]));
        vi.stubGlobal('fetch', fetch);
        const old = useToolPresentationStore.getState().load('s');
        const revision = useToolPresentationStore.getState().revision;
        useToolPresentationStore.getState().activate('s', true);
        expect(useToolPresentationStore.getState().revision).toBe(revision + 1);
        expect(fetch.mock.calls[0][1].signal.aborted).toBe(true);
        await useToolPresentationStore.getState().load('s');
        resolveOld(page([{ runId: 'r1', toolUseId: 'same-id', assistantMessageId: 'm1', text: 'stale', sequence: 1 }]));
        await old;
        const state = useToolPresentationStore.getState();
        expect(state.loaded).toBe(true);
        expect(selectToolPresentation(state, 's', 'same-id', undefined, 'm2')).toBe('current');
        expect(selectToolPresentation(state, 's', 'same-id', undefined, 'm1')).toBe('');
    });
});
