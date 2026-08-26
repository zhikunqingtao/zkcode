/**
 * wsClient 测试 — S11 原生 WebSocket 客户端（stompClient.ts 同路径替换版）
 *
 * 覆盖：连接/重连退避序列、bind 流程、应用层 ping、17 导出函数上行 JSON
 * 形状（type 逐字对齐 zk-protocol client_message.rs）、下行白名单过滤 +
 * dispatch 调用、10min 重连放弃。
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// ==================== dispatch 模块 mock（隔离消费侧） ====================

const dispatchMock = vi.fn();
const resetSequenceMock = vi.fn();
const resetBoundSessionMock = vi.fn();
const bindSessionAndWaitMock = vi.fn(
    (sessionId: string, publish: (payload: {
        sessionId: string; protocolVersion: number;
        bindRequestId: string; bindingEpoch: number;
    }) => void | boolean) => {
        publish({
            sessionId,
            protocolVersion: 3,
            bindRequestId: 'bind-req-test',
            bindingEpoch: 1,
        });
        return Promise.resolve(true);
    },
);

vi.mock('./dispatch', () => ({
    dispatch: (...args: unknown[]) => dispatchMock(...args),
    resetSequence: () => resetSequenceMock(),
    resetBoundSession: () => resetBoundSessionMock(),
    bindSessionAndWait: (...args: unknown[]) =>
        (bindSessionAndWaitMock as unknown as (...a: unknown[]) => Promise<boolean>)(...args),
}));

// ==================== 原生 WebSocket mock ====================

class MockWebSocket {
    static instances: MockWebSocket[] = [];
    static CONNECTING = 0;
    static OPEN = 1;
    static CLOSING = 2;
    static CLOSED = 3;

    url: string;
    readyState = MockWebSocket.CONNECTING;
    sent: string[] = [];
    onopen: (() => void) | null = null;
    onmessage: ((event: { data: string }) => void) | null = null;
    onclose: (() => void) | null = null;
    onerror: (() => void) | null = null;

    constructor(url: string) {
        this.url = url;
        MockWebSocket.instances.push(this);
    }

    send(data: string): void {
        this.sent.push(data);
    }

    close(): void {
        this.readyState = MockWebSocket.CLOSED;
    }

    /** 测试辅助：模拟连接建立 */
    simulateOpen(): void {
        this.readyState = MockWebSocket.OPEN;
        this.onopen?.();
    }

    /** 测试辅助：模拟服务端/网络断开 */
    simulateClose(): void {
        this.readyState = MockWebSocket.CLOSED;
        this.onclose?.();
    }

    /** 测试辅助：模拟下行帧 */
    simulateMessage(payload: unknown): void {
        this.onmessage?.({
            data: typeof payload === 'string' ? payload : JSON.stringify(payload),
        });
    }

    lastSentJson(): Record<string, unknown> {
        return JSON.parse(this.sent[this.sent.length - 1]);
    }
}

vi.stubGlobal('WebSocket', MockWebSocket);

import {
    createStompClient,
    disconnectStomp,
    getStompClient,
    isConnected,
    isWsConnected,
    send,
    sendInterrupt,
    sendMcpOperation,
    sendPing,
    sendRewindFiles,
    sendRunInput,
    sendSetModel,
    sendSetPermissionMode,
    sendSlashCommand,
    sendToServer,
    sendUserMessage,
    waitForWsConnection,
} from './stompClient';
import { useSessionStore } from '@/store/sessionStore';

function lastSocket(): MockWebSocket {
    const instance = MockWebSocket.instances[MockWebSocket.instances.length - 1];
    expect(instance).toBeDefined();
    return instance;
}

/** 建立一条已打开的连接（默认无活跃会话，跳过 bind） */
function connectAndOpen(): MockWebSocket {
    createStompClient('', '');
    const ws = lastSocket();
    ws.simulateOpen();
    return ws;
}

describe('native WS client', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        MockWebSocket.instances = [];
        dispatchMock.mockClear();
        resetSequenceMock.mockClear();
        resetBoundSessionMock.mockClear();
        bindSessionAndWaitMock.mockClear();
        useSessionStore.setState({ sessionId: null });
    });

    afterEach(() => {
        disconnectStomp();
        vi.useRealTimers();
    });

    // ==================== 上行 JSON 形状（type 逐字） ====================

    describe('upstream envelope shapes', () => {
        it('maps all 15 destinations to the backend type whitelist verbatim', () => {
            const ws = connectAndOpen();
            const cases: Array<[string, object, string]> = [
                ['/app/chat', { text: 'hi' }, 'user_message'],
                ['/app/run-input', { requestId: 'r', text: 't' }, 'run_input'],
                ['/app/permission', { toolUseId: 'u', decision: 'allow', remember: false, scope: 'session' }, 'permission_response'],
                ['/app/interrupt', {}, 'interrupt'],
                ['/app/model', { model: 'm' }, 'set_model'],
                ['/app/permission-mode', { mode: 'DEFAULT' }, 'set_permission_mode'],
                ['/app/command', { command: 'help', args: '' }, 'slash_command'],
                ['/app/mcp', { operation: 'list', serverId: 's' }, 'mcp_operation'],
                ['/app/rewind', { messageId: 'm', filePaths: [] }, 'rewind_files'],
                ['/app/elicitation', { requestId: 'r', answer: 'a' }, 'elicitation_response'],
                ['/app/ping', {}, 'ping'],
                ['/app/bind-session', { sessionId: 's', bindRequestId: 'b', bindingEpoch: 1, protocolVersion: 3 }, 'bind_session'],
                ['/app/interaction-received', { interactionId: 'i', deliveryGeneration: 2 }, 'interaction_ack'],
                ['/app/activity-save', { id: 'a', operationType: 'op' }, 'activity_save'],
                ['/app/activity-update', { id: 'a', decision: 'approved' }, 'activity_update'],
            ];
            for (const [destination, body, expectedType] of cases) {
                expect(sendToServer(destination, body)).toBe(true);
                const frame = ws.lastSentJson();
                expect(frame.type).toBe(expectedType);
                expect(frame).toMatchObject(body);
            }
        });

        it('passes attachment url through the user_message payload verbatim', () => {
            const ws = connectAndOpen();

            sendUserMessage('see image', [{
                type: 'image', name: 'shot.png', mediaType: 'image/png',
                url: 'https://oss.example.com/shot.png',
            }], []);
            expect(ws.lastSentJson()).toEqual({
                type: 'user_message', text: 'see image',
                attachments: [{
                    type: 'image', name: 'shot.png', mediaType: 'image/png',
                    url: 'https://oss.example.com/shot.png',
                }],
                references: [],
            });
        });

        it('serializes each convenience sender with exact type and camelCase fields', () => {
            const ws = connectAndOpen();

            sendUserMessage('hello', [{ type: 'image', mediaType: 'image/png', base64Data: 'x' } as never], [{ type: 'file', path: '/a.ts' }]);
            expect(ws.lastSentJson()).toEqual({
                type: 'user_message', text: 'hello',
                attachments: [{ type: 'image', mediaType: 'image/png', base64Data: 'x' }],
                references: [{ type: 'file', path: '/a.ts' }],
            });

            expect(sendRunInput('req-1', 'more')).toBe(true);
            expect(ws.lastSentJson()).toEqual({ type: 'run_input', requestId: 'req-1', text: 'more' });

            sendInterrupt();
            expect(ws.lastSentJson()).toEqual({ type: 'interrupt' });

            sendSetModel('kimi');
            expect(ws.lastSentJson()).toEqual({ type: 'set_model', model: 'kimi' });

            expect(sendSetPermissionMode('AUTO_APPROVE')).toBe(true);
            expect(ws.lastSentJson()).toEqual({ type: 'set_permission_mode', mode: 'AUTO_APPROVE' });

            expect(sendSlashCommand('compact', '')).toBe(true);
            expect(ws.lastSentJson()).toEqual({ type: 'slash_command', command: 'compact', args: '' });

            sendMcpOperation('connect', 'srv-1', { url: 'x' });
            expect(ws.lastSentJson()).toEqual({
                type: 'mcp_operation', operation: 'connect', serverId: 'srv-1', config: { url: 'x' },
            });

            sendRewindFiles('msg-1', ['a.ts', 'b.ts']);
            expect(ws.lastSentJson()).toEqual({ type: 'rewind_files', messageId: 'msg-1', filePaths: ['a.ts', 'b.ts'] });

            sendPing();
            expect(ws.lastSentJson()).toEqual({ type: 'ping' });
        });

        it('defaults interaction_ack deliveryGeneration to 1 when the caller omits it', () => {
            const ws = connectAndOpen();
            expect(sendToServer('/app/interaction-received', { interactionId: 'i-1' })).toBe(true);
            expect(ws.lastSentJson()).toEqual({
                type: 'interaction_ack', interactionId: 'i-1', deliveryGeneration: 1,
            });
        });

        it('drops unknown destinations without sending', () => {
            const ws = connectAndOpen();
            const before = ws.sent.length;
            expect(sendToServer('/app/does-not-exist', { a: 1 })).toBe(false);
            send('/app/does-not-exist', { a: 1 });
            expect(ws.sent.length).toBe(before);
        });

        it('refuses to send while disconnected', () => {
            expect(sendToServer('/app/chat', { text: 'x' })).toBe(false);
            createStompClient('', '');
            // 未 open：CONNECTING 状态同样拒发
            expect(sendToServer('/app/chat', { text: 'x' })).toBe(false);
            expect(lastSocket().sent.length).toBe(0);
        });
    });

    // ==================== bind 流程与应用层心跳 ====================

    describe('bind flow and application ping', () => {
        it('re-binds the active session on connect and starts heartbeat after bind', async () => {
            useSessionStore.setState({ sessionId: 'sess-1' });
            const ws = connectAndOpen();

            expect(resetSequenceMock).toHaveBeenCalledTimes(1);
            expect(resetBoundSessionMock).toHaveBeenCalledTimes(1);
            expect(bindSessionAndWaitMock).toHaveBeenCalledWith('sess-1', expect.any(Function));
            expect(JSON.parse(ws.sent[0])).toEqual({
                type: 'bind_session',
                sessionId: 'sess-1',
                bindRequestId: 'bind-req-test',
                bindingEpoch: 1,
                protocolVersion: 3,
            });

            // bind promise 落定后启动 10s 心跳
            await vi.advanceTimersByTimeAsync(0);
            await vi.advanceTimersByTimeAsync(10_000);
            expect(ws.lastSentJson()).toEqual({ type: 'ping' });
            const count = ws.sent.length;
            await vi.advanceTimersByTimeAsync(10_000);
            expect(ws.sent.length).toBe(count + 1);
        });

        it('starts heartbeat immediately when no session is active', async () => {
            const ws = connectAndOpen();
            expect(bindSessionAndWaitMock).not.toHaveBeenCalled();
            await vi.advanceTimersByTimeAsync(10_000);
            expect(ws.lastSentJson()).toEqual({ type: 'ping' });
        });

        it('stops heartbeat after the socket closes', async () => {
            const ws = connectAndOpen();
            await vi.advanceTimersByTimeAsync(10_000);
            const count = ws.sent.length;
            ws.simulateClose();
            await vi.advanceTimersByTimeAsync(30_000);
            expect(ws.sent.length).toBe(count);
        });
    });

    // ==================== 下行分发 ====================

    describe('downstream dispatch', () => {
        it('feeds whitelisted messages to dispatch with routing fields intact', () => {
            const ws = connectAndOpen();
            const frame = {
                type: 'stream_delta', content: 'x',
                ts: 42, seq: 7, _sessionId: 'sess-1', _bindingEpoch: 3,
            };
            ws.simulateMessage(frame);
            expect(dispatchMock).toHaveBeenCalledWith(frame);

            const pong = { type: 'pong', bindRequired: true, serverNow: 1234, ts: 43 };
            ws.simulateMessage(pong);
            expect(dispatchMock).toHaveBeenCalledWith(pong);
        });

        it('filters unknown types and non-JSON frames', () => {
            const ws = connectAndOpen();
            ws.simulateMessage({ type: 'totally_unknown', ts: 1 });
            ws.simulateMessage('h');
            ws.simulateMessage('not-json{');
            ws.simulateMessage('');
            expect(dispatchMock).not.toHaveBeenCalled();
        });
    });

    // ==================== 重连退避与 10min 放弃 ====================

    describe('reconnect backoff', () => {
        it('retries with 1s→2s→4s→8s→10s cap backoff', async () => {
            const first = connectAndOpen();
            first.simulateClose();

            const delays = [1000, 2000, 4000, 8000, 10_000, 10_000];
            for (const delay of delays) {
                const countBefore = MockWebSocket.instances.length;
                await vi.advanceTimersByTimeAsync(delay - 1);
                expect(MockWebSocket.instances.length).toBe(countBefore);
                await vi.advanceTimersByTimeAsync(1);
                expect(MockWebSocket.instances.length).toBe(countBefore + 1);
                lastSocket().simulateClose();
            }
        });

        it('resets the backoff after a successful reconnect', async () => {
            const first = connectAndOpen();
            first.simulateClose();
            await vi.advanceTimersByTimeAsync(1000);
            const second = lastSocket();
            second.simulateOpen();

            // 重连成功后再次断开 → 退避回到 1s 起步
            second.simulateClose();
            const countBefore = MockWebSocket.instances.length;
            await vi.advanceTimersByTimeAsync(1000);
            expect(MockWebSocket.instances.length).toBe(countBefore + 1);
        });

        it('gives up after the 10min reconnect window and notifies the caller', async () => {
            const first = connectAndOpen();
            first.simulateClose();
            await vi.advanceTimersByTimeAsync(1000);

            // 越过 10min 窗口后的下一次断开 → 放弃重连
            vi.setSystemTime(Date.now() + 11 * 60 * 1000);
            const waiter = waitForWsConnection();
            const rejection = expect(waiter).rejects.toThrow('重连超时');
            lastSocket().simulateClose();
            await rejection;

            expect(isConnected()).toBe(false);
            const countAfter = MockWebSocket.instances.length;
            await vi.advanceTimersByTimeAsync(60_000);
            expect(MockWebSocket.instances.length).toBe(countAfter);
        });
    });

    // ==================== 生命周期与句柄 ====================

    describe('lifecycle and handle', () => {
        it('exposes active/connected via the compat handle', () => {
            expect(getStompClient()).toBeNull();
            const handle = createStompClient('', '');
            expect(getStompClient()).toBe(handle);
            expect(handle.active).toBe(true);
            expect(handle.connected).toBe(false);
            expect(isConnected()).toBe(true);
            expect(isWsConnected()).toBe(false);

            lastSocket().simulateOpen();
            expect(handle.connected).toBe(true);
            expect(isWsConnected()).toBe(true);

            disconnectStomp();
            expect(handle.active).toBe(false);
            expect(getStompClient()).toBeNull();
            expect(isConnected()).toBe(false);
        });

        it('routes handle.publish through the destination mapping', () => {
            const ws = connectAndOpen();
            getStompClient()?.publish({
                destination: '/app/bind-session',
                body: JSON.stringify({ sessionId: 's', bindRequestId: 'b', bindingEpoch: 1, protocolVersion: 3 }),
            });
            expect(ws.lastSentJson()).toEqual({
                type: 'bind_session', sessionId: 's', bindRequestId: 'b', bindingEpoch: 1, protocolVersion: 3,
            });
        });

        it('does not reconnect after an intentional disconnect', async () => {
            connectAndOpen();
            disconnectStomp();
            const count = MockWebSocket.instances.length;
            await vi.advanceTimersByTimeAsync(60_000);
            expect(MockWebSocket.instances.length).toBe(count);
        });
    });
});
