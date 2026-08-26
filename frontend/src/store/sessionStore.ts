/**
 * SessionStore — 会话状态管理
 * SPEC: §8.3 Store #1
 * 持久化: 当前标签页的 sessionId 使用 sessionStorage，其他状态不持久化
 */

import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { subscribeWithSelector } from 'zustand/middleware';
import { usePermissionStore } from './permissionStore';

const ACTIVE_SESSION_KEY = 'zkcode.activeSessionId';

function readActiveSessionId(): string | null {
    if (typeof window === 'undefined') return null;
    try {
        return window.sessionStorage.getItem(ACTIVE_SESSION_KEY);
    } catch {
        return null;
    }
}

function saveActiveSessionId(sessionId: string): void {
    if (typeof window === 'undefined') return;
    try {
        if (sessionId) {
            window.sessionStorage.setItem(ACTIVE_SESSION_KEY, sessionId);
        } else {
            window.sessionStorage.removeItem(ACTIVE_SESSION_KEY);
        }
    } catch {
        // Storage may be unavailable (for example, browser privacy settings).
    }
}

export interface SessionStoreState {
    // 状态
    sessionId: string | null;
    model: string | null;
    status: 'idle' | 'streaming' | 'waiting_permission' | 'compacting';
    turnCount: number;
    effortValue: number;
    isAborted: boolean;

    // Actions
    createSession: (
        projectId: string | null,
        model: string | null,
    ) => Promise<string>;
    resumeSession: (sessionId: string) => Promise<void>;
    setModel: (model: string) => void;
    setEffort: (value: number) => void;
    setStatus: (status: SessionStoreState['status']) => void;
    handleRateLimit: (data: { retryAfterMs: number; limitType: string }) => void;
    abort: () => void;
}

export const useSessionStore = create<SessionStoreState>()(
    subscribeWithSelector(immer((set) => ({
        // 初始值
        sessionId: readActiveSessionId(),
        model: null,
        status: 'idle' as const,
        turnCount: 0,
        effortValue: 3,
        isAborted: false,

        // Actions
        createSession: async (projectId, model) => {
            // null 字段语义（Phase 1 Rust 后端无 Project/config 域）：
            // projectId === null → 创建未绑定 Project 的 Session；
            // model === null → 不携带 model，由服务端默认模型裁定。
            if (projectId !== null && !projectId.trim()) {
                throw new Error('创建 Session 前必须选择已授权的 Project');
            }
            const payload: Record<string, string> = {};
            if (projectId !== null) payload.projectId = projectId;
            if (model !== null) payload.model = model;
            const resp = await fetch('/api/sessions', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            if (!resp.ok) {
                throw new Error(`HTTP ${resp.status}`);
            }
            const body = await resp.json() as {
                sessionId?: unknown;
            };
            if (typeof body.sessionId !== 'string'
                    || body.sessionId.trim() === '') {
                throw new Error(
                    '服务端返回了无效的 Session');
            }
            // 新建对话默认授予完全访问权限，避免逐次工具确认打断首轮对话。
            usePermissionStore.getState().setPermissionMode('auto_approve');
            // REST creation only yields a candidate. The active Session and
            // sessionStorage are committed by the matching session_restored
            // frame after WebSocket binding succeeds.
            return body.sessionId;
        },
        resumeSession: async (sessionId) => {
            set(d => { d.sessionId = sessionId; d.status = 'idle'; });
            saveActiveSessionId(sessionId);
        },
        setModel: (model) => set(d => { d.model = model; }),
        setEffort: (value) => set(d => { d.effortValue = value; }),
        setStatus: (status) => set(d => { d.status = status; }),
        handleRateLimit: (_data) => set(d => { d.status = 'idle'; }),
        abort: () => set(d => { d.isAborted = true; d.status = 'idle'; }),
    })))
);
