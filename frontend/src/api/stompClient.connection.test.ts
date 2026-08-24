import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// 隔离消费侧 — stompClient 仅依赖这四个 dispatch 导出
vi.mock('./dispatch', () => ({
    dispatch: vi.fn(),
    resetSequence: vi.fn(),
    resetBoundSession: vi.fn(),
    bindSessionAndWait: vi.fn(() => Promise.resolve(true)),
}));

class MockWebSocket {
    static instances: MockWebSocket[] = [];
    static OPEN = 1;

    url: string;
    readyState = 0;
    onopen: (() => void) | null = null;
    onmessage: ((event: { data: string }) => void) | null = null;
    onclose: (() => void) | null = null;
    onerror: (() => void) | null = null;

    constructor(url: string) {
        this.url = url;
        MockWebSocket.instances.push(this);
    }

    send(): void { /* no-op */ }

    close(): void {
        this.readyState = 3;
    }

    simulateOpen(): void {
        this.readyState = MockWebSocket.OPEN;
        this.onopen?.();
    }
}

vi.stubGlobal('WebSocket', MockWebSocket);

import {
    createStompClient,
    disconnectStomp,
    waitForWsConnection,
} from './stompClient';

describe('WS connection readiness', () => {
    beforeEach(() => {
        MockWebSocket.instances = [];
        vi.clearAllMocks();
    });

    afterEach(() => {
        disconnectStomp();
    });

    it('resumes waiters only after the socket is open', async () => {
        const readiness = waitForWsConnection();
        let settled = false;
        void readiness.then(() => { settled = true; });

        createStompClient('', '');
        expect(settled).toBe(false);

        const socket = MockWebSocket.instances[0];
        expect(socket).toBeDefined();
        // onmessage 在 open 前已挂好 — 立即 bind 不会丢下行响应
        expect(socket.onmessage).toBeTypeOf('function');
        socket.simulateOpen();
        await readiness;

        expect(settled).toBe(true);
    });

    it('allows a superseded caller to cancel its connection wait', async () => {
        const controller = new AbortController();
        const readiness = waitForWsConnection(controller.signal);

        controller.abort();

        await expect(readiness).rejects.toMatchObject({
            name: 'AbortError',
        });
    });
});
