/**
 * NotificationStore — 通知状态管理
 * SPEC: §8.3 Store #9
 * 持久化: 否
 */

import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { subscribeWithSelector } from 'zustand/middleware';
import type { NotificationItem, NotificationPriority } from '@/types';

type NotificationConfig = {
    key: string;
    level: 'info' | 'success' | 'warning' | 'error';
    message: string;
    priority?: NotificationPriority;
    timeout?: number;
    /** §10.7-②：错误 Toast 的可执行重试动作（有则渲染“重试”主钮） */
    onRetry?: () => void;
};

export interface NotificationStoreState {
    notifications: NotificationItem[];
    /** Memory only: switching sessions hides these notices without dismissing them. */
    cancellationNotices: Record<string, { sessionId: string; runId: string; notification: NotificationItem; dismissed: boolean }>;

    addNotification: (config: NotificationConfig) => void;
    addCancellationNotice: (sessionId: string, runId: string, config: NotificationConfig) => void;
    showCancellationNotices: (sessionId: string, runId: string | null) => void;
    forgetCancellationNotice: (sessionId: string, runId: string) => void;
    removeNotification: (key: string) => void;
    clearAll: () => void;
}

export const useNotificationStore = create<NotificationStoreState>()(
    subscribeWithSelector(immer((set) => ({
        notifications: [],
        cancellationNotices: {},

        addNotification: (config) => set(d => {
            // key 是通知身份；替换后移到末尾，进入最新通知的可见窗口。
            d.notifications = d.notifications.filter(n => n.key !== config.key);
            d.notifications.push({
                key: config.key,
                level: config.level,
                message: config.message,
                priority: config.priority ?? 'normal',
                timeout: config.timeout ?? 5000,
                createdAt: Date.now(),
                onRetry: config.onRetry,
            });
        }),
        addCancellationNotice: (sessionId, runId, config) => set(d => {
            const notification = {
                ...config, priority: config.priority ?? 'normal',
                timeout: config.timeout ?? 0, createdAt: Date.now(),
            };
            const identity = `${sessionId}:${runId}`;
            const dismissed = d.cancellationNotices[identity]?.dismissed ?? false;
            d.cancellationNotices[identity] = { sessionId, runId, notification, dismissed };
            d.notifications = d.notifications.filter(n => n.key !== config.key);
            if (!dismissed) d.notifications.push(notification);
        }),
        showCancellationNotices: (sessionId, runId) => set(d => {
            d.notifications = d.notifications.filter(n => !n.key.startsWith('cancellation-pending:'));
            for (const notice of Object.values(d.cancellationNotices)) {
                if (notice.sessionId === sessionId && notice.runId === runId && !notice.dismissed) {
                    d.notifications.push(notice.notification);
                }
            }
        }),
        forgetCancellationNotice: (sessionId, runId) => set(d => {
            delete d.cancellationNotices[`${sessionId}:${runId}`];
            d.notifications = d.notifications.filter(n => n.key !== `cancellation-pending:${runId}`);
        }),
        removeNotification: (key) => set(d => {
            d.notifications = d.notifications.filter(n => n.key !== key);
            for (const notice of Object.values(d.cancellationNotices)) {
                if (notice.notification.key === key) notice.dismissed = true;
            }
        }),
        clearAll: () => set(d => { d.notifications = []; d.cancellationNotices = {}; }),
    })))
);
