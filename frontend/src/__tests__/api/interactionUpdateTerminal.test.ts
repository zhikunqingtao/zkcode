import { beforeEach, describe, expect, it, vi } from 'vitest';
import { bindSessionAndWait, dispatch, resetBoundSession } from '@/api/dispatch';
import { useAppUiStore } from '@/store/appUiStore';
import { useCostStore } from '@/store/costStore';
import { useMessageStore } from '@/store/messageStore';
import { useNotificationStore } from '@/store/notificationStore';
import { usePermissionStore } from '@/store/permissionStore';
import { useRunStore } from '@/store/runStore';
import { useSessionStore } from '@/store/sessionStore';

const sendToServerMock = vi.hoisted(() => vi.fn(() => true));
vi.mock('@/api/stompClient', () => ({
    send: vi.fn(),
    sendToServer: sendToServerMock,
}));

type BindPayload = { sessionId: string; protocolVersion: number; bindRequestId: string; bindingEpoch: number };

/** 完成一次 bind → session_restored 握手，使会话进入已绑定状态。 */
async function bindSession(sessionId: string): Promise<void> {
    let payload: BindPayload | undefined;
    const bound = bindSessionAndWait(sessionId, value => { payload = value; });
    dispatch({
        type: 'session_restored', bindRequestId: payload!.bindRequestId, protocolVersion: 3,
        bindingEpoch: payload!.bindingEpoch, messages: [],
        metadata: { sessionId, model: 'model', permissionMode: 'DEFAULT', status: 'idle' },
    } as never);
    await expect(bound).resolves.toBe(true);
}

function showPendingPermission(deadline?: number): void {
    usePermissionStore.getState().showPermission({
        interactionId: 'perm-1', toolUseId: 'tool-1', toolName: 'Write',
        input: {}, riskLevel: 'medium', reason: 'test',
        decisionDeadlineAt: deadline, version: 1,
    });
}

describe('interaction_updated / interaction_terminal dispatch', () => {
    beforeEach(() => {
        sendToServerMock.mockClear();
        sendToServerMock.mockReturnValue(true);
        resetBoundSession();
        window.sessionStorage.clear();
        useSessionStore.setState({ sessionId: null, status: 'idle' });
        useMessageStore.getState().clearMessages();
        useMessageStore.setState({ activeToolCalls: new Map() });
        usePermissionStore.getState().clearPermissions();
        useAppUiStore.setState({ elicitationDialog: null });
        useNotificationStore.getState().clearAll();
        useCostStore.setState({
            sessionCost: 0,
            totalCost: 0,
            usage: { inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
        });
        useRunStore.setState({ recoverySnapshots: new Map(), recoveryEventSeq: new Map() });
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, json: async () => [] }));
    });

    it('bypasses the recovery filter and updates the pending permission deadline immediately', async () => {
        const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            await bindSession('session-live');
            showPendingPermission(Date.now() + 5_000);

            // 再次 bind 打开恢复门（activeRecoveryId 生效），但不立即回 session_restored。
            let gatePayload: BindPayload | undefined;
            const gated = bindSessionAndWait('session-live', value => { gatePayload = value; });

            // 非豁免消息被恢复门暂存，不立即生效。
            dispatch({
                type: 'cost_update', sessionCost: 7, totalCost: 9,
                usage: { inputTokens: 1, outputTokens: 1, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
            } as never);
            expect(useCostStore.getState().sessionCost).toBe(0);

            // interaction_updated 属 RECOVERY_BYPASS_TYPES，恢复期也必须立即更新 deadline。
            const serverNow = Date.now();
            const before = Date.now();
            dispatch({
                type: 'interaction_updated', interactionId: 'perm-1',
                decisionDeadlineAt: serverNow + 30_000, serverNow, version: 2,
            } as never);
            const after = Date.now();

            const pending = usePermissionStore.getState().pendingPermissions[0];
            expect(pending?.decisionDeadlineAt).toBeGreaterThanOrEqual(before + 30_000);
            expect(pending?.decisionDeadlineAt).toBeLessThanOrEqual(after + 30_000);
            expect(pending?.version).toBe(2);

            // 关门后暂存帧按序重放，证明上面的 cost_update 是被暂存而非丢弃。
            dispatch({
                type: 'session_restored', bindRequestId: gatePayload!.bindRequestId, protocolVersion: 3,
                bindingEpoch: gatePayload!.bindingEpoch, messages: [],
                metadata: { sessionId: 'session-live', model: 'model', permissionMode: 'DEFAULT', status: 'idle' },
            } as never);
            await expect(gated).resolves.toBe(true);
            expect(useCostStore.getState().sessionCost).toBe(7);
        } finally {
            warnSpy.mockRestore();
        }
    });

    it('warns and leaves both stores untouched when the deadline cannot be parsed', () => {
        showPendingPermission(111);
        useAppUiStore.getState().showElicitationDialog({
            interactionId: 'elic-1', requestId: 'elic-1',
            question: 'Q?', options: [], decisionDeadlineAt: 222, version: 1,
        });
        const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            dispatch({
                type: 'interaction_updated', interactionId: 'perm-1',
                decisionDeadlineAt: 'not-a-timestamp', serverNow: Date.now(), version: 5,
            } as never);

            expect(warnSpy).toHaveBeenCalledWith(
                '[WS] interaction_updated: deadline computation failed, store not updated',
                expect.objectContaining({
                    interactionId: 'perm-1',
                    decisionDeadlineAt: 'not-a-timestamp',
                }));
            const pending = usePermissionStore.getState().pendingPermissions[0];
            expect(pending?.decisionDeadlineAt).toBe(111);
            expect(pending?.version).toBe(1);
            expect(useAppUiStore.getState().elicitationDialog?.decisionDeadlineAt).toBe(222);
        } finally {
            warnSpy.mockRestore();
        }
    });

    it('warns without updating stores when the deadline field is missing entirely', () => {
        showPendingPermission(333);
        const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            dispatch({
                type: 'interaction_updated', interactionId: 'perm-1',
                serverNow: Date.now(), version: 6,
            } as never);

            expect(warnSpy).toHaveBeenCalledWith(
                '[WS] interaction_updated: deadline computation failed, store not updated',
                expect.objectContaining({ interactionId: 'perm-1' }));
            const pending = usePermissionStore.getState().pendingPermissions[0];
            expect(pending?.decisionDeadlineAt).toBe(333);
            expect(pending?.version).toBe(1);
        } finally {
            warnSpy.mockRestore();
        }
    });

    it('interaction_terminal removes the pending permission and keeps the unrelated elicitation dialog', () => {
        showPendingPermission();
        useAppUiStore.getState().showElicitationDialog({
            interactionId: 'elic-1', requestId: 'elic-1', question: 'Q?', options: [],
        });

        dispatch({
            type: 'interaction_terminal', interactionId: 'perm-1', interactionType: 'permission',
        } as never);

        expect(usePermissionStore.getState().pendingPermissions).toEqual([]);
        expect(useAppUiStore.getState().elicitationDialog?.interactionId).toBe('elic-1');
    });

    it('interaction_terminal dismisses the elicitation dialog only on a matching interactionId', () => {
        useAppUiStore.getState().showElicitationDialog({
            interactionId: 'elic-1', requestId: 'elic-1', question: 'Q?', options: [],
        });

        dispatch({
            type: 'interaction_terminal', interactionId: 'elic-other', interactionType: 'elicitation',
        } as never);
        expect(useAppUiStore.getState().elicitationDialog?.interactionId).toBe('elic-1');

        dispatch({
            type: 'interaction_terminal', interactionId: 'elic-1', interactionType: 'elicitation',
        } as never);
        expect(useAppUiStore.getState().elicitationDialog).toBeNull();
    });

    it('does not restore activeToolCalls when the run snapshot status is terminal', async () => {
        let payload: BindPayload | undefined;
        const bound = bindSessionAndWait('session-done', value => { payload = value; });
        dispatch({
            type: 'session_restored', bindRequestId: payload!.bindRequestId, protocolVersion: 3,
            bindingEpoch: payload!.bindingEpoch, messages: [],
            metadata: { sessionId: 'session-done', model: 'model', permissionMode: 'DEFAULT', status: 'idle' },
            runSnapshot: { id: 'run-done', status: 'COMPLETED' },
            snapshotEventSeq: 7,
            activeToolCalls: [{ toolUseId: 'tool-done', toolName: 'Bash', input: { command: 'ls' } }],
        } as never);
        await expect(bound).resolves.toBe(true);

        // 终态 Run 的 activeToolCalls 是陈旧投影，必须丢弃，避免 UI 显示幽灵工具。
        expect(useMessageStore.getState().activeToolCalls.size).toBe(0);
        expect(useSessionStore.getState().status).toBe('idle');
    });
});
