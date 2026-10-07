import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
    dispatch,
    isSessionBound,
    resetBoundSession,
} from '@/api/dispatch';
import {
    isWsConnected,
    sendToServer,
    waitForWsConnection,
} from '@/api/stompClient';
import { useMessageStore } from '@/store/messageStore';
import { useSessionStore } from '@/store/sessionStore';
import { useCostStore } from '@/store/costStore';
import { useNotificationStore } from '@/store/notificationStore';
import { usePromptDraftStore, capturePromptDraftTarget } from '@/store/promptDraftStore';
import type { Message } from '@/types';
import { runtimeEnvelope } from '@/test/runtimeEnvelope';
import {
    activateSessionCandidate,
    captureSessionSelectionGuard,
    clearSessionSelection,
    getPendingSessionActivation,
} from './sessionActivation';

vi.mock('@/api/stompClient', () => ({
    isWsConnected: vi.fn(() => true),
    sendToServer: vi.fn(() => true),
    waitForWsConnection: vi.fn(() => Promise.resolve()),
}));

interface BindPayload {
    sessionId: string;
    protocolVersion: number;
    bindRequestId: string;
    bindingEpoch: number;
}

const oldMessage: Message = {
    uuid: 'old-message',
    type: 'user',
    content: [{ type: 'text', text: 'keep old state' }],
    timestamp: 1,
};

function restore(payload: BindPayload, messages: Message[] = []): void {
    dispatch({
            ...runtimeEnvelope(),
        type: 'session_restored',
        protocolVersion: 4,
        bindRequestId: payload.bindRequestId,
        bindingEpoch: payload.bindingEpoch,
        messages,
        metadata: {
            sessionId: payload.sessionId,
            model: 'test-model',
            permissionMode: 'DEFAULT',
            status: 'idle',
        },
    } as never);
}

describe('Session activation transaction', () => {
    beforeEach(async () => {
        vi.useFakeTimers();
        vi.mocked(isWsConnected).mockReturnValue(true);
        vi.mocked(sendToServer).mockReset();
        vi.mocked(waitForWsConnection).mockReset();
        vi.mocked(waitForWsConnection).mockResolvedValue();
        resetBoundSession();
        window.sessionStorage.clear();
        usePromptDraftStore.setState({ drafts: {} });
        useNotificationStore.getState().clearAll();
        useMessageStore.getState().clearMessages();
        useCostStore.getState().resetSessionCost();
        useSessionStore.setState({
            sessionId: null,
            model: null,
            status: 'idle',
        });
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
            ok: true,
            json: async () => [],
        }));
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.unstubAllGlobals();
        vi.clearAllMocks();
    });

    it('keeps the old Session until a timed-out switch is safely rebound', async () => {
        await useSessionStore.getState().resumeSession('session-old');
        useMessageStore.getState().addMessage(oldMessage);
        const binds: BindPayload[] = [];
        vi.mocked(sendToServer).mockImplementation((_destination, body) => {
            const payload = body as BindPayload;
            binds.push(payload);
            if (payload.sessionId === 'session-old') {
                restore(payload, [oldMessage]);
            }
            return true;
        });

        const activation = activateSessionCandidate('session-new', {
            bindTimeoutMs: 50,
        });

        expect(useSessionStore.getState().sessionId).toBe('session-old');
        expect(useMessageStore.getState().messages).toEqual([oldMessage]);
        expect(window.sessionStorage.getItem('zkcode.activeSessionId'))
            .toBe('session-old');

        await vi.advanceTimersByTimeAsync(50);
        await expect(activation).resolves.toMatchObject({ status: 'failed' });
        expect(binds).toHaveLength(2);
        expect(binds[1].sessionId).toBe('session-old');
        expect(binds[1].bindingEpoch).toBeGreaterThan(
            binds[0].bindingEpoch,
        );
        expect(useSessionStore.getState().sessionId).toBe('session-old');
        expect(useMessageStore.getState().messages).toEqual([oldMessage]);
        expect(isSessionBound('session-old')).toBe(true);

        // A restore from the timed-out candidate is no longer authoritative.
        restore(binds[0], [{ ...oldMessage, uuid: 'late-new-state' }]);
        expect(useSessionStore.getState().sessionId).toBe('session-old');
        expect(useMessageStore.getState().messages).toEqual([oldMessage]);
    });

    it('keeps the latest selection pending beyond the old connection timeout', async () => {
        let connect: (() => void) | undefined;
        vi.mocked(isWsConnected).mockReturnValue(false);
        vi.mocked(waitForWsConnection).mockImplementation(() =>
            new Promise<void>(resolve => { connect = resolve; }));
        vi.mocked(sendToServer).mockImplementation((_destination, body) => {
            restore(body as BindPayload, []);
            return true;
        });

        const activation = activateSessionCandidate('session-delayed');
        await vi.advanceTimersByTimeAsync(5_000);

        expect(sendToServer).not.toHaveBeenCalled();
        expect(getPendingSessionActivation()).toBe(activation);
        expect(useSessionStore.getState().sessionId).toBeNull();

        vi.mocked(isWsConnected).mockReturnValue(true);
        connect?.();
        await expect(activation).resolves.toEqual({
            status: 'activated',
            sessionId: 'session-delayed',
        });
        expect(sendToServer).toHaveBeenCalledTimes(1);
        expect(useSessionStore.getState().sessionId)
            .toBe('session-delayed');
    });

    it('cancels a disconnected selection when a newer selection wins', async () => {
        const waits: Array<{
            resolve: () => void;
            reject: (error: Error) => void;
        }> = [];
        vi.mocked(isWsConnected).mockReturnValue(false);
        vi.mocked(waitForWsConnection).mockImplementation(signal =>
            new Promise<void>((resolve, reject) => {
                waits.push({ resolve, reject });
                signal?.addEventListener('abort', () => {
                    const error = new Error('superseded');
                    error.name = 'AbortError';
                    reject(error);
                }, { once: true });
            }));
        vi.mocked(sendToServer).mockImplementation((_destination, body) => {
            restore(body as BindPayload, []);
            return true;
        });

        const first = activateSessionCandidate('session-a');
        await Promise.resolve();
        const second = activateSessionCandidate('session-b');

        await expect(first).resolves.toEqual({
            status: 'superseded',
            sessionId: 'session-a',
        });
        expect(waits).toHaveLength(2);

        vi.mocked(isWsConnected).mockReturnValue(true);
        waits[1].resolve();
        await expect(second).resolves.toEqual({
            status: 'activated',
            sessionId: 'session-b',
        });
        expect(sendToServer).toHaveBeenCalledTimes(1);
        expect(useSessionStore.getState().sessionId).toBe('session-b');
    });

    it('lets a newer switch win over an older in-flight result', async () => {
        await useSessionStore.getState().resumeSession('session-old');
        useMessageStore.getState().addMessage(oldMessage);
        const binds: BindPayload[] = [];
        vi.mocked(sendToServer).mockImplementation((_destination, body) => {
            const payload = body as BindPayload;
            binds.push(payload);
            if (payload.sessionId === 'session-b') {
                restore(payload, []);
            }
            return true;
        });

        const first = activateSessionCandidate('session-a');
        await Promise.resolve();
        const second = activateSessionCandidate('session-b');

        await expect(first).resolves.toEqual({
            status: 'superseded',
            sessionId: 'session-a',
        });
        await expect(second).resolves.toEqual({
            status: 'activated',
            sessionId: 'session-b',
        });
        expect(useSessionStore.getState().sessionId).toBe('session-b');
        expect(isSessionBound('session-b')).toBe(true);

        const stale = binds.find(bind => bind.sessionId === 'session-a');
        expect(stale).toBeDefined();
        restore(stale!, [oldMessage]);
        dispatch({
            ...runtimeEnvelope(),
            type: 'protocol_error',
            code: 'SESSION_NOT_FOUND',
            supportedVersion: 3,
            bindRequestId: stale!.bindRequestId,
            bindingEpoch: stale!.bindingEpoch,
        } as never);
        expect(useSessionStore.getState().sessionId).toBe('session-b');
        expect(useMessageStore.getState().messages).toEqual([]);
    });

    it('lets a send path await the candidate already being activated', async () => {
        await useSessionStore.getState().resumeSession('session-old');
        let candidateBind: BindPayload | undefined;
        vi.mocked(sendToServer).mockImplementation((_destination, body) => {
            candidateBind = body as BindPayload;
            return true;
        });

        const switching = activateSessionCandidate('session-new');
        await Promise.resolve();
        const sendReadiness = getPendingSessionActivation();

        expect(sendReadiness).toBe(switching);
        expect(candidateBind?.sessionId).toBe('session-new');
        restore(candidateBind!, []);
        await expect(sendReadiness).resolves.toEqual({
            status: 'activated',
            sessionId: 'session-new',
        });
        expect(vi.mocked(sendToServer)).toHaveBeenCalledTimes(1);
        expect(useSessionStore.getState().sessionId).toBe('session-new');
    });

    it('accepts a matching restore even when interaction recovery is slow', async () => {
        vi.mocked(fetch).mockImplementation(() => new Promise(() => {}));
        vi.mocked(sendToServer).mockImplementation((_destination, body) => {
            restore(body as BindPayload, []);
            return true;
        });

        const activation = activateSessionCandidate('session-restored', {
            bindTimeoutMs: 50,
        });
        await Promise.resolve();
        dispatch({
            ...runtimeEnvelope(),
            type: 'cost_update',
            sessionCost: 7,
            totalCost: 9,
            usage: {
                inputTokens: 1,
                outputTokens: 2,
                cacheReadInputTokens: 0,
                cacheCreationInputTokens: 0,
            },
        } as never);
        await vi.advanceTimersByTimeAsync(50);

        await expect(activation).resolves.toEqual({
            status: 'activated',
            sessionId: 'session-restored',
        });
        expect(useSessionStore.getState().sessionId)
            .toBe('session-restored');
        expect(isSessionBound('session-restored')).toBe(true);
        expect(useCostStore.getState().sessionCost).toBe(7);
    });
    it('commits a captured new-session draft before authoritative session publication', async () => {
        usePromptDraftStore.getState().setInput('__none__', 'home with pending assets');
        usePromptDraftStore.getState().setLocalFiles('__none__', [{ path: '/project/a.txt', name: 'a.txt', size: 1 }]);
        const id = usePromptDraftStore.getState().drafts.__none__.id;
        const resolveAttachmentTarget = capturePromptDraftTarget('__none__');
        const publishedDrafts: Array<string | undefined> = [];
        const unsubscribe = useSessionStore.subscribe(state => {
            if (state.sessionId === 'created') publishedDrafts.push(usePromptDraftStore.getState().drafts.created?.id);
        });
        let bind!: BindPayload;
        vi.mocked(sendToServer).mockImplementation((_dest, body) => { bind = body as BindPayload; return true; });
        const pending = activateSessionCandidate('created', { newSessionDraftId: id });
        await Promise.resolve();
        expect(usePromptDraftStore.getState().drafts.created).toBeUndefined();
        expect(bind).not.toHaveProperty('newSessionDraftId');
        restore({ ...bind, bindingEpoch: bind.bindingEpoch + 1 });
        expect(usePromptDraftStore.getState().drafts.__none__.id).toBe(id);
        restore(bind);
        await expect(pending).resolves.toMatchObject({ status: 'activated' });
        expect(publishedDrafts.length).toBeGreaterThan(0);
        expect(publishedDrafts.every(value => value === id)).toBe(true);
        expect(resolveAttachmentTarget()).toBe('created');
        expect(usePromptDraftStore.getState().drafts.created.localFiles[0].path).toBe('/project/a.txt');
        // An async attachment completion still follows its own draft after another selection.
        useSessionStore.setState({ sessionId: 'other' });
        const attachment = { id: 'late', name: 'late.png', size: 1, type: 'image/png', file: new File(['x'], 'late.png') };
        usePromptDraftStore.getState().setAttachments(resolveAttachmentTarget()!, [attachment]);
        expect(usePromptDraftStore.getState().drafts.created.attachments[0].id).toBe('late');
        expect(usePromptDraftStore.getState().drafts.other).toBeUndefined();
        unsubscribe();
    });

    it('ordinary restore preserves independent home and existing-session drafts', async () => {
        usePromptDraftStore.getState().setInput('__none__', 'home');
        usePromptDraftStore.getState().setInput('existing', 'existing');
        const drafts = usePromptDraftStore.getState().drafts;
        vi.mocked(sendToServer).mockImplementation((_dest, body) => { restore(body as BindPayload); return true; });
        await expect(activateSessionCandidate('existing')).resolves.toMatchObject({ status: 'activated' });
        expect(usePromptDraftStore.getState().drafts).toEqual(drafts);
    });

    it('failed and superseded new binds cannot transfer the captured home draft', async () => {
        usePromptDraftStore.getState().setInput('__none__', 'keep home');
        const id = usePromptDraftStore.getState().drafts.__none__.id;
        const binds: BindPayload[] = [];
        vi.mocked(sendToServer).mockImplementation((_dest, body) => { binds.push(body as BindPayload); return true; });
        const first = activateSessionCandidate('created-first', { newSessionDraftId: id, bindTimeoutMs: 20 });
        await Promise.resolve();
        const second = activateSessionCandidate('existing-second', { bindTimeoutMs: 20 });
        await Promise.resolve();
        restore(binds[0]);
        expect(usePromptDraftStore.getState().drafts.__none__.id).toBe(id);
        await vi.advanceTimersByTimeAsync(25);
        await expect(first).resolves.toMatchObject({ status: 'superseded' });
        await expect(second).resolves.toMatchObject({ status: 'failed' });
        expect(Object.keys(usePromptDraftStore.getState().drafts)).toEqual(['__none__']);
    });

    it.each(['replacement-home', 'occupied-target'])('confirmed creation preserves independent drafts on %s', async (conflict) => {
        usePromptDraftStore.getState().setInput('__none__', 'original home');
        const id = usePromptDraftStore.getState().drafts.__none__.id;
        let bind!: BindPayload;
        vi.mocked(sendToServer).mockImplementation((_destination, body) => { bind = body as BindPayload; return true; });
        const pending = activateSessionCandidate('created', { newSessionDraftId: id });
        await Promise.resolve();
        if (conflict === 'replacement-home') {
            usePromptDraftStore.getState().clear('__none__');
            usePromptDraftStore.getState().setInput('__none__', 'replacement home');
        } else {
            usePromptDraftStore.getState().setInput('created', 'independent target');
        }
        const drafts = usePromptDraftStore.getState().drafts;
        restore(bind);
        await expect(pending).resolves.toMatchObject({ status: 'activated' });
        expect(usePromptDraftStore.getState().drafts).toEqual(drafts);
        expect(useNotificationStore.getState().notifications).toEqual([
            expect.objectContaining({
                key: `prompt-draft-transfer:${id}:created`,
                level: 'warning',
                message: expect.stringContaining('草稿未自动转移'),
            }),
        ]);
    });

    it.each([false, true])('reconfirming a transferred draft identity does not report a conflict (new home: %s)', async replacement => {
        usePromptDraftStore.getState().setInput('__none__', 'original home');
        const id = usePromptDraftStore.getState().drafts.__none__.id;
        vi.mocked(sendToServer).mockImplementation((_destination, body) => { restore(body as BindPayload); return true; });
        await expect(activateSessionCandidate('created', { newSessionDraftId: id })).resolves.toMatchObject({ status: 'activated' });
        if (replacement) usePromptDraftStore.getState().setInput('__none__', 'new independent home');
        const drafts = usePromptDraftStore.getState().drafts;
        resetBoundSession();
        await expect(activateSessionCandidate('created', { newSessionDraftId: id })).resolves.toMatchObject({ status: 'activated' });
        expect(usePromptDraftStore.getState().drafts).toEqual(drafts);
        expect(useNotificationStore.getState().notifications).toEqual([]);
    });

    it('expires a creation guard as soon as a newer history selection starts', async () => {
        const creationStillCurrent = captureSessionSelectionGuard();
        expect(creationStillCurrent()).toBe(true);
        vi.mocked(sendToServer).mockImplementation((_destination, body) => {
            restore(body as BindPayload);
            return true;
        });
        const history = activateSessionCandidate('selected-history');
        expect(creationStillCurrent()).toBe(false);
        await expect(history).resolves.toMatchObject({ status: 'activated' });
        expect(useSessionStore.getState().sessionId).toBe('selected-history');
        expect(creationStillCurrent()).toBe(false);
    });

    it('discarded new-session activation cannot commit a late restore after returning home', async () => {
        usePromptDraftStore.getState().setInput('__none__', 'keep home');
        const id = usePromptDraftStore.getState().drafts.__none__.id;
        let bind!: BindPayload;
        vi.mocked(sendToServer).mockImplementation((_destination, body) => { bind = body as BindPayload; return true; });
        const activation = activateSessionCandidate('abandoned-new-session', { newSessionDraftId: id });
        await Promise.resolve();
        clearSessionSelection();
        restore(bind);
        expect(useSessionStore.getState().sessionId).toBe('');
        expect(usePromptDraftStore.getState().drafts.__none__.id).toBe(id);
        expect(usePromptDraftStore.getState().drafts['abandoned-new-session']).toBeUndefined();
        await expect(activation).resolves.toMatchObject({ status: 'superseded' });
    });

});
