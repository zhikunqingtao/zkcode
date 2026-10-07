import { useSyncExternalStore } from 'react';
import { usePermissionStore } from '@/store/permissionStore';
import { useSessionStore } from '@/store/sessionStore';
import { useNotificationStore } from '@/store/notificationStore';
import { isSessionBindingReady, subscribeSessionBinding } from '@/api/dispatch';
import { sendSetPermissionMode } from '@/api/stompClient';
import type { PermissionMode } from '@/types';
import { generateUUID } from '@/utils/uuid';

/** Both permission controls display only server-confirmed values. */
export function useSessionPermissionSelection() {
    const purpose = useSessionStore(s => s.purpose);
    const permissionMode = usePermissionStore(s => s.permissionMode);
    const pending = usePermissionStore(s => s.pendingModeChange);
    const message = usePermissionStore(s => s.modeChangeMessage);
    const sessionId = useSessionStore(s => s.sessionId);
    const bindingReady = useSyncExternalStore(subscribeSessionBinding,
        () => !!sessionId && isSessionBindingReady(sessionId), () => false);
    const selectMode = (mode: PermissionMode) => {
        if (useSessionStore.getState().purpose === 'mcp' || usePermissionStore.getState().pendingModeChange) return;
        const currentSessionId = useSessionStore.getState().sessionId;
        const notify = (key: string, text: string) => {
            const notifications = useNotificationStore.getState();
            notifications.removeNotification(key);
            notifications.addNotification({ key, level: 'error', message: text });
        };
        if (!currentSessionId || currentSessionId !== sessionId || !isSessionBindingReady(currentSessionId)) {
            notify('permission-mode-no-session', '请先创建或选择已连接的会话，再切换权限模式');
            return;
        }
        const request = { sessionId: currentSessionId, mode, startedAt: Date.now(), requestId: generateUUID() };
        usePermissionStore.setState({ pendingModeChange: request, modeChangeMessage: null });
        let sent = false;
        try { sent = sendSetPermissionMode(mode.toUpperCase(), request.requestId); } catch { /* handled below */ }
        if (!sent) {
            usePermissionStore.getState().clearModeChange();
            notify('permission-mode-send-failed', '权限模式切换发送失败，请检查连接后重试');
            return;
        }
        window.setTimeout(() => {
            if (usePermissionStore.getState().pendingModeChange?.requestId !== request.requestId) return;
            usePermissionStore.setState({ pendingModeChange: null, modeChangeMessage: '切换结果尚未确认' });
        }, 10000);
    };
    return { permissionMode: purpose === 'mcp' ? 'default' as const : permissionMode, selectMode, pending: pending !== null, disabled: purpose === 'mcp' || pending !== null || !bindingReady, message: purpose === 'mcp' ? 'MCP 专用会话固定 DEFAULT；写入需在 Activity 单独确认。' : message };
}
