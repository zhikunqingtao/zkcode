import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { bindSessionAndWait, dispatch, isSessionBindingReady, recoverPendingInteractions, resetBoundSession } from '@/api/dispatch';
import { runtimeEnvelope } from '@/test/runtimeEnvelope';
import { useSessionStore } from '@/store/sessionStore';
import { useNotificationStore } from '@/store/notificationStore';
import { usePermissionStore } from '@/store/permissionStore';
import { useMessageStore } from '@/store/messageStore';

vi.mock('@/api/stompClient', () => ({ sendToServer: vi.fn(() => true) }));
type Bind = Parameters<Parameters<typeof bindSessionAndWait>[1]>[0];
function restore(bind: Bind) {
    dispatch({ ...runtimeEnvelope({ sessionId: bind.sessionId }), _bindingEpoch: bind.bindingEpoch,
        type: 'session_restored', bindRequestId: bind.bindRequestId, bindingEpoch: bind.bindingEpoch,
        protocolVersion: 4, messages: [], metadata: { sessionId: bind.sessionId, model: 'm', permissionMode: 'DEFAULT', status: 'running' } });
}
beforeEach(() => {
    resetBoundSession();
    useSessionStore.setState({ sessionId: null, status: 'idle' });
    useMessageStore.getState().clearMessages();
    useNotificationStore.getState().clearAll();
    usePermissionStore.getState().clearPermissions();
});
afterEach(() => { resetBoundSession(); vi.useRealTimers(); vi.unstubAllGlobals(); });

describe('authoritative bind and cancellation diagnostics', () => {
    it('finishes a WS snapshot without a second HTTP interaction request', async () => {
        vi.useFakeTimers();
        const fetcher = vi.fn(() => new Promise(() => {}));
        vi.stubGlobal('fetch', fetcher);
        let bind!: Bind;
        const done = bindSessionAndWait('s', payload => { bind = payload; });
        restore(bind);
        expect(isSessionBindingReady('s')).toBe(true);
        expect(fetcher).not.toHaveBeenCalled();
        await expect(done).resolves.toBe(true);
    });

    it('rebinds overflow once with a new epoch and does not accept the old snapshot', async () => {
        vi.useFakeTimers();
        vi.spyOn(console, 'warn').mockImplementation(() => {});
        const binds: Bind[] = [];
        const done = bindSessionAndWait('s', payload => { binds.push(payload); });
        const first = binds[0];
        for (let i = 0; i <= 5000; i++) dispatch({ ...runtimeEnvelope({ sessionId: 's' }),
            _bindingEpoch: first.bindingEpoch, type: 'stream_delta', messageId: 'stale', delta: 'stale' });
        expect(binds).toHaveLength(2);
        expect(binds[1].bindingEpoch).toBeGreaterThan(first.bindingEpoch);
        restore(first);
        expect(isSessionBindingReady('s')).toBe(false);
        restore(binds[1]);
        await expect(done).resolves.toBe(true);
        expect(useMessageStore.getState().streamingContent).toBe('');
    });

    it('bounds manual refresh to eight seconds including a stalled response body', async () => {
        vi.useFakeTimers();
        let bind!: Bind;
        const done = bindSessionAndWait('s', payload => { bind = payload; });
        restore(bind); await done;
        let signal: AbortSignal | undefined;
        vi.stubGlobal('fetch', vi.fn((_url, init: RequestInit) => { signal = init.signal as AbortSignal;
            return Promise.resolve({ ok: true, json: () => new Promise(() => {}) }); }));
        const refresh = recoverPendingInteractions('s');
        const failure = expect(refresh).rejects.toThrow('INTERACTION_RECOVERY_TIMEOUT');
        await vi.advanceTimersByTimeAsync(8000);
        await failure;
        expect(signal?.aborted).toBe(true);
    });

    it('fails a second overflow without a third bind or replaying buffered events', async () => {
        vi.useFakeTimers();
        const binds: Bind[] = [];
        const done = bindSessionAndWait('s', payload => { binds.push(payload); });
        for (let attempt = 0; attempt < 2; attempt++) {
            const bind = binds[attempt];
            for (let i = 0; i <= 5000; i++) dispatch({ ...runtimeEnvelope({ sessionId: 's' }),
                _bindingEpoch: bind.bindingEpoch, type: 'stream_delta', messageId: 'stale', delta: 'stale' });
        }
        await expect(done).resolves.toBe(false);
        expect(binds).toHaveLength(2);
        expect(isSessionBindingReady('s')).toBe(false);
        expect(useNotificationStore.getState().notifications).toEqual(expect.arrayContaining([expect.objectContaining({ key: 'session-sync-failed' })]));
        expect(useMessageStore.getState().streamingContent).toBe('');
    });

    it('keeps the original five-second deadline when overflow triggers a new binding epoch', async () => {
        vi.useFakeTimers();
        const binds: Bind[] = [];
        const done = bindSessionAndWait('s', payload => { binds.push(payload); });
        await vi.advanceTimersByTimeAsync(4500);
        for (let i = 0; i <= 5000; i++) dispatch({ ...runtimeEnvelope({ sessionId: 's' }),
            _bindingEpoch: binds[0].bindingEpoch, type: 'stream_delta', messageId: 'stale', delta: 'stale' });
        expect(binds).toHaveLength(2);
        await vi.advanceTimersByTimeAsync(500);
        await expect(done).resolves.toBe(false);
        restore(binds[1]);
        expect(isSessionBindingReady('s')).toBe(false);
    });

    it('does not treat interruption acknowledgement as a terminal result', async () => {
        let bind!: Bind;
        const done = bindSessionAndWait('s', payload => { bind = payload; });
        restore(bind); await done;
        useSessionStore.getState().setStatus('streaming');
        dispatch({ ...runtimeEnvelope({ sessionId: 's', runId: 'r', sourceRunId: 'r' }),
            _bindingEpoch: bind.bindingEpoch, type: 'interrupt_ack', reason: 'USER_INTERRUPT' });
        expect(useSessionStore.getState().status).toBe('streaming');
        expect(useMessageStore.getState().messages.some(message => message.type === 'system' && message.subtype === 'interrupt')).toBe(false);
    });
    it('keeps pending diagnostics non-terminal, clears them only for their Run, and ignores late replay', async () => {
        let bind!: Bind;
        const done = bindSessionAndWait('pending-session', payload => { bind = payload; });
        restore(bind); await done;
        useSessionStore.getState().setStatus('streaming');
        const notice = (runId: string, sessionId = 'pending-session') => dispatch({
            ...runtimeEnvelope({ sessionId, runId, sourceRunId: runId }), _bindingEpoch: bind.bindingEpoch,
            type: 'notification', key: `cancellation-pending:${runId}`, level: 'warning', message: 'still reconciling', timeout: 0,
        });
        notice('r1'); notice('r2'); notice('foreign', 'other-session');
        expect(useNotificationStore.getState().notifications.map(item => item.key)).toEqual(['cancellation-pending:r1', 'cancellation-pending:r2']);
        expect(useSessionStore.getState().status).toBe('streaming');
        dispatch({ ...runtimeEnvelope({ sessionId: 'pending-session', runId: 'r1', sourceRunId: 'r1' }), _bindingEpoch: bind.bindingEpoch,
            type: 'message_complete', usage: { inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 }, stopReason: 'cancelled' });
        await Promise.resolve();
        notice('r1');
        expect(useNotificationStore.getState().notifications.map(item => item.key)).toEqual(['cancellation-pending:r2']);
    });

    it('does not resurrect an interaction closed by WS during manual HTTP refresh', async () => {
        let bind!: Bind;
        const done = bindSessionAndWait('s', payload => { bind = payload; });
        restore(bind); await done;
        let resolve!: (value: unknown) => void;
        vi.stubGlobal('fetch', vi.fn(() => Promise.resolve({ ok: true, json: () => new Promise(done => { resolve = done; }) })));
        const refresh = recoverPendingInteractions('s');
        expect(recoverPendingInteractions('s')).toBe(refresh);
        await Promise.resolve();
        dispatch({ ...runtimeEnvelope({ sessionId: 's' }), _bindingEpoch: bind.bindingEpoch, type: 'interaction_terminal', interactionId: 'p', interactionType: 'permission', status: 'resolved', version: 2 });
        resolve([{ protocolVersion: 3, sessionId: 's', interactionId: 'p', status: 'pending', interactionType: 'permission', prompt: {}, deliveryGeneration: 1 }]);
        await refresh;
        expect(usePermissionStore.getState().pendingPermissions).toEqual([]);
    });

});
