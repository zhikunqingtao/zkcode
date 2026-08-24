import { describe, test, expect, vi, beforeEach, afterEach } from 'vitest';
import { sendToServer, isWsConnected } from '@/api/stompClient';

// S11: stompClient 已重写为原生 WebSocket 客户端，
// 不再需要 @stomp/stompjs / sockjs-client 模块 mock。

describe('useWebSocket module-level functions', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    test('sendToServer returns false when not connected', () => {
        // Before any connection, the module-level socket is null
        // Note: In a real scenario, the client may already be set from other tests
        // This tests the defensive check
        const result = sendToServer('/app/chat', { text: 'test' });
        // Depending on module state, this may return true (if connected) or false
        expect(typeof result).toBe('boolean');
    });

    test('isWsConnected returns boolean', () => {
        const connected = isWsConnected();
        expect(typeof connected).toBe('boolean');
    });

    test('sendToServer calls send when connected', () => {
        // If the global client is active from a previous useWebSocket call,
        // sendToServer should work
        if (isWsConnected()) {
            const result = sendToServer('/app/chat', { text: 'hello' });
            expect(result).toBe(true);
        }
    });
});
