import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { bindSessionAndWait, dispatch, resetBoundSession } from '@/api/dispatch';
import { runtimeEnvelope } from '@/test/runtimeEnvelope';
import { useSessionStore } from '@/store/sessionStore';
import { useNotificationStore } from '@/store/notificationStore';
import type { RuntimeRunSnapshot } from '@/types';

vi.mock('@/api/stompClient', () => ({ sendToServer: vi.fn(() => true) }));
type Bind = Parameters<Parameters<typeof bindSessionAndWait>[1]>[0];
type Status = RuntimeRunSnapshot['status'];
let serial = 0;
let sessionId: string;
let runId: string;

async function restore(session: string, run: string, status: Status) {
    let bind!: Bind;
    const done = bindSessionAndWait(session, value => { bind = value; });
    dispatch({ ...runtimeEnvelope({ sessionId: session }), _bindingEpoch: bind.bindingEpoch,
        type: 'session_restored', ...bind, protocolVersion: 4, messages: [],
        metadata: { sessionId: session, model: 'm', permissionMode: 'DEFAULT', status: 'idle' },
        runSnapshot: { id: run, status, verificationStatus: 'notRequested' },
    });
    await expect(done).resolves.toBe(true);
    return bind;
}

function warn(bind: Bind, run: string) {
    dispatch({ ...runtimeEnvelope({ sessionId: bind.sessionId, runId: run, sourceRunId: run }),
        _bindingEpoch: bind.bindingEpoch, type: 'notification', key: `cancellation-pending:${run}`,
        level: 'warning', message: 'still reconciling', timeout: 0,
    });
}

const keys = () => useNotificationStore.getState().notifications.map(item => item.key);

beforeEach(() => {
    serial += 1;
    sessionId = `cancel-session-${serial}`;
    runId = `cancel-run-${serial}`;
    resetBoundSession();
    useSessionStore.setState({ sessionId: null, status: 'idle' });
    useNotificationStore.getState().clearAll();
    vi.stubGlobal('fetch', vi.fn(() => { throw new Error('unexpected network request'); }));
});
afterEach(() => { resetBoundSession(); vi.unstubAllGlobals(); });

describe('cancellation warning recovery', () => {
    it.each(['running', 'cancelling'] as const)('keeps the same %s Run warning on reconnect', async status => {
        warn(await restore(sessionId, runId, status), runId);
        resetBoundSession();
        await restore(sessionId, runId, status);
        expect(useSessionStore.getState().status).toBe('streaming');
        expect(keys()).toContain(`cancellation-pending:${runId}`);
    });

    it('hides another session warning and restores it when returning', async () => {
        warn(await restore(sessionId, runId, 'cancelling'), runId);
        await restore(`${sessionId}-other`, `${runId}-other`, 'running');
        expect(keys()).not.toContain(`cancellation-pending:${runId}`);
        await restore(sessionId, runId, 'cancelling');
        expect(keys()).toContain(`cancellation-pending:${runId}`);
    });

    it.each(['completed', 'failed', 'cancelled', 'interrupted'] as const)('clears %s and rejects a late warning', async status => {
        warn(await restore(sessionId, runId, 'cancelling'), runId);
        const bind = await restore(sessionId, runId, status);
        expect(keys()).not.toContain(`cancellation-pending:${runId}`);
        warn(bind, runId);
        expect(keys()).not.toContain(`cancellation-pending:${runId}`);
    });

    it('does not restore or replay a warning from an older Run', async () => {
        warn(await restore(sessionId, runId, 'cancelling'), runId);
        const bind = await restore(sessionId, `${runId}-next`, 'running');
        warn(bind, runId);
        expect(keys()).not.toContain(`cancellation-pending:${runId}`);
    });

    it('does not invent a warning from cancelling alone', async () => {
        await restore(sessionId, runId, 'cancelling');
        expect(keys()).toEqual([]);
    });

    it('respects an explicitly dismissed warning after reconnect', async () => {
        warn(await restore(sessionId, runId, 'cancelling'), runId);
        useNotificationStore.getState().removeNotification(`cancellation-pending:${runId}`);
        await restore(sessionId, runId, 'cancelling');
        expect(keys()).toEqual([]);
    });

    it('rejects a dismissed old Run warning after a newer authoritative snapshot', async () => {
        warn(await restore(sessionId, runId, 'cancelling'), runId);
        useNotificationStore.getState().removeNotification(`cancellation-pending:${runId}`);
        const bind = await restore(sessionId, `${runId}-next`, 'running');
        warn(bind, runId);
        expect(keys()).toEqual([]);
    });
});
