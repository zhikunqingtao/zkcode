import { beforeEach, describe, expect, it, vi } from 'vitest';
import { bindSessionAndWait, dispatch, resetBoundSession } from '@/api/dispatch';
import { usePermissionStore } from '@/store/permissionStore';
import { useSessionStore } from '@/store/sessionStore';

const sendToServerMock = vi.hoisted(() => vi.fn(() => true));
vi.mock('@/api/stompClient', () => ({
    send: vi.fn(),
    sendToServer: sendToServerMock,
}));

const SESSION = '7a7aa3bd-ff91-4b2b-b541-19e00b99803b';
const RUN = 'c84c8a0f-1b4f-412f-b726-bd34f32a4101';
const HASH = '36cbfcd90c336b3eea9fa5ee8be24cb20ed7f5b9a5ee3e9fa56746bd9bfad7c2';
const INTERACTION = 'e257fba7-956d-4aef-8758-81cb09bc11d2';

/**
 * DEFAULT 权限模式下 Rust hub 为真实故障行序列化出的 `interaction_created`
 * 帧（逐字取自 interaction_requests 生产数据），用于锁定「后端投递 → 前端弹窗
 * → 回发 ACK」这条链路：任何一环回归都会让交互在 30s 后判 UNDELIVERABLE，
 * 表现为工具显示完成但实际从未执行。
 */
const FRAME = {
    type: 'interaction_created',
    interactionId: INTERACTION,
    protocolVersion: 3,
    correlationKey: `permission-v3:Bash_0:${HASH}`,
    sessionId: SESSION,
    runId: RUN,
    interactionType: 'permission',
    status: 'pending',
    prompt: {
        inputSummary: 'pwd',
        operationHash: HASH,
        reason: 'Read access requires confirmation',
        riskLevel: 'safe',
        toolName: 'Bash',
        toolUseId: 'Bash_0',
    },
    allowedDecisions: ['allow', 'deny'],
    scopeOptions: ['run', 'session'],
    source: 'direct',
    actorRunId: RUN,
    actorType: 'direct',
    deliveryGeneration: 1,
    dispatchAttempts: 1,
    createdAt: 1786928516314,
    deliveryWindowEndsAt: 1786928546314,
    version: 1,
    serverNow: 1786928516314,
    operationHash: HASH,
    options: [
        { optionId: 'allow_once', decision: 'allow', scope: 'once' },
        { optionId: 'allow_run', decision: 'allow', scope: 'run' },
        { optionId: 'allow_session', decision: 'allow', scope: 'session' },
        { optionId: 'deny', decision: 'deny', scope: 'once' },
    ],
    ts: 1786928516314,
    seq: 12,
    _sessionId: SESSION,
    _bindingEpoch: 1,
} as unknown as Parameters<typeof dispatch>[0];

/** 完成一次 bind → session_restored 握手，返回服务端应回显的 bindingEpoch。 */
async function bindSession(): Promise<number> {
    let payload: { bindRequestId: string; bindingEpoch: number } | undefined;
    const bound = bindSessionAndWait(SESSION, value => { payload = value as never; });
    dispatch({
        type: 'session_restored', bindRequestId: payload!.bindRequestId, protocolVersion: 3,
        bindingEpoch: payload!.bindingEpoch, messages: [],
        metadata: { sessionId: SESSION, model: 'model', permissionMode: 'DEFAULT', status: 'idle' },
    } as never);
    await expect(bound).resolves.toBe(true);
    return payload!.bindingEpoch;
}

describe('permission interaction delivery', () => {
    beforeEach(() => {
        sendToServerMock.mockClear();
        resetBoundSession();
        usePermissionStore.setState({ pendingPermissions: [] });
        useSessionStore.setState({ sessionId: SESSION });
    });

    it('shows the permission dialog and acks the delivery generation once bound', async () => {
        const bindingEpoch = await bindSession();

        dispatch({ ...(FRAME as object), _bindingEpoch: bindingEpoch } as never);

        expect(usePermissionStore.getState().pendingPermissions).toHaveLength(1);
        expect(useSessionStore.getState().status).toBe('waiting_permission');
        await vi.waitFor(() => expect(sendToServerMock).toHaveBeenCalledWith(
            '/app/interaction-received',
            expect.objectContaining({ interactionId: INTERACTION, deliveryGeneration: 1 }),
        ));
    });

    it('drops a frame carrying a stale bindingEpoch without prompting or acking', async () => {
        const bindingEpoch = await bindSession();
        // 独立 interactionId：markSessionBound 会按设计重放上一场景遗留的 ACK，
        // 断言必须锁定本帧自身，避免被那次补发污染。
        const staleId = 'f3b1c07e-0000-4000-8000-000000000001';

        dispatch({
            ...(FRAME as object), interactionId: staleId, _bindingEpoch: bindingEpoch - 1,
        } as never);

        expect(usePermissionStore.getState().pendingPermissions).toHaveLength(0);
        expect(sendToServerMock).not.toHaveBeenCalledWith(
            '/app/interaction-received',
            expect.objectContaining({ interactionId: staleId }),
        );
    });

    it('warns with the drop reason instead of silently discarding a non-pending frame', async () => {
        const bindingEpoch = await bindSession();
        const droppedId = 'a0e0c07e-0000-4000-8000-000000000002';
        const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        try {
            dispatch({
                ...(FRAME as object), interactionId: droppedId,
                status: 'decided', _bindingEpoch: bindingEpoch,
            } as never);

            expect(usePermissionStore.getState().pendingPermissions).toHaveLength(0);
            expect(warnSpy).toHaveBeenCalledWith('[WS] interaction_created dropped:',
                expect.objectContaining({
                    interactionId: droppedId,
                    status: 'decided',
                    reason: 'status is not pending',
                }));
        } finally {
            warnSpy.mockRestore();
        }
    });
});
