/**
 * 原生 WebSocket 客户端 — S11 协议层改造（同路径替换原 SockJS/STOMP 客户端）
 * 对接 zk-server 原生 WS：`GET /ws` + 扁平 JSON 信封
 *
 * 功能:
 * - 原生 `new WebSocket(url)`（不再依赖 sockjs-client / @stomp/stompjs）
 * - 上行：顶层 `type` 路由（替代 STOMP destination），15 个 type 逐字对齐
 *   zk-protocol `client_message.rs` 白名单
 * - 心跳：服务端协议层每 10s 发 Ping 帧（浏览器自动回 Pong，无需处理）；
 *   应用层 bind 完成后每 10s 发 `ping` 上行（对齐旧 D1 双轨心跳）
 * - 断线自动重连（指数退避 1s→2s→4s→8s→10s cap，10min 总超时放弃）
 * - 下行：MessageEvent → JSON.parse → 类型白名单校验 → dispatch
 *
 * 兼容性：保留原 stompClient.ts 的 17 个导出函数签名与语义，
 * 消费侧 dispatch.ts / handlers / stores 零改动。
 */

import { bindSessionAndWait, dispatch, resetSequence, resetBoundSession } from './dispatch';
import { useSessionStore } from '@/store/sessionStore';
import { useBridgeStore } from '@/store/bridgeStore';
import { useNotificationStore } from '@/store/notificationStore';
import type { ServerMessage, Attachment } from '@/types';

/**
 * 合法消息类型白名单 — 从 dispatch.ts handlers 和 ServerMessage 类型推断
 * （与 zk-protocol server_message.rs 的 57 个下行 type 逐字一致）
 */
const VALID_MESSAGE_TYPES: ReadonlySet<string> = new Set([
    'stream_delta', 'thinking_delta', 'tool_use_start', 'tool_use_input', 'tool_use_progress', 'tool_result',
    'error', 'compact_complete', 'message_complete', 'compact_start', 'rate_limit',
    'permission_request', 'tool_permission_denied', 'cost_update', 'task_update', 'agent_spawn', 'agent_update',
    'agent_complete', 'agent_started', 'agent_completed', 'agent_failed',
    'elicitation', 'prompt_suggestion', 'speculation_result',
    'bridge_status', 'notification', 'teammate_message', 'mcp_tool_update',
    'mcp_tool_progress',
    'mcp_health_status', 'session_restored', 'pong', 'compact_event', 'token_warning',
    'interrupt_ack', 'run_input_queued', 'run_input_applied', 'run_input_rejected',
    'model_changed', 'model_routed', 'permission_mode_changed', 'command_result',
    'rewind_complete', 'token_budget_nudge', 'plan_update',
    'swarm_state_update', 'worker_progress', 'workflow_phase_update',
    'session_list_updated',
    // 差异化升级 v1.5 §4.5 C: 结构化输出自动可视化消息
    'visualization',
    // APOS: 验证结果 + 验证进度推送
    'verification_result',
    'verify_progress',
    // RV-4: 证据包待审批通知（统一在主通道分发）
    'verify_attention', 'protocol_error', 'interaction_created', 'interaction_terminal', 'interaction_updated',
]);

/**
 * 旧 STOMP destination → 新协议上行 type 映射（15 条，逐字对齐
 * zk-protocol client_message.rs 的 `ClientMessage::kind()` 输出域）。
 * 保留 destination 入参签名，调用方（含 dispatch.ts 动态 import）零改动。
 */
const DESTINATION_TYPE_MAP: Readonly<Record<string, string>> = {
    '/app/chat': 'user_message',
    '/app/run-input': 'run_input',
    '/app/permission': 'permission_response',
    '/app/interrupt': 'interrupt',
    '/app/model': 'set_model',
    '/app/permission-mode': 'set_permission_mode',
    '/app/command': 'slash_command',
    '/app/mcp': 'mcp_operation',
    '/app/rewind': 'rewind_files',
    '/app/elicitation': 'elicitation_response',
    '/app/ping': 'ping',
    '/app/bind-session': 'bind_session',
    '/app/interaction-received': 'interaction_ack',
    '/app/activity-save': 'activity_save',
    '/app/activity-update': 'activity_update',
};

/**
 * 防御性消息解析 — 原生 WS 下行帧恒为单条 JSON 文本；
 * 保留空帧防御与类型白名单校验（P-FE-02 / P1-08 语义），
 * 移除 SockJS 数组帧 / STOMP 帧提取降级（协议不再产生）。
 */
function parseMessage(raw: string): (ServerMessage & { ts?: number }) | null {
    // 防御 null/undefined/空字符串
    if (!raw || raw.trim() === '') {
        return null;
    }

    try {
        const payload = JSON.parse(raw);

        // payload.type 有效性校验
        if (payload && payload.type && !VALID_MESSAGE_TYPES.has(payload.type)) {
            console.debug('[WS] Unknown message type, skipping:', payload.type);
            return null;
        }

        return payload;
    } catch {
        // 非 JSON 消息 — 解析错误降为 debug 级别
        if (raw.length > 2) {
            console.debug('[WS] Non-JSON message ignored:', raw.substring(0, 80));
        }
        return null;
    }
}

/** 重连延迟配置 */
const RECONNECT_DELAY_INITIAL = 1000;    // 初始 1s
const RECONNECT_DELAY_MAX = 10000;       // 最大 10s
const RECONNECT_TIMEOUT = 10 * 60 * 1000; // 重连超时 10min

/** WebSocket.OPEN 常量（本地定值，避免测试环境 global 常量缺失） */
const WS_OPEN = 1;

/** 客户端单例状态 */
let socket: WebSocket | null = null;
let wsHandle: WsClientHandle | null = null;
let clientActive = false;          // createStompClient 后 true；deactivate/超时放弃后 false
let currentAuthToken = '';
let reconnectAttempts = 0;
let reconnectStartTime = 0;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
let applicationPingTimer: ReturnType<typeof setInterval> | null = null;

// ==================== 连接等待器（语义与旧实现一致） ====================

interface ConnectionWaiter {
    resolve: () => void;
    reject: (error: Error) => void;
    signal?: AbortSignal;
    abortHandler?: () => void;
}

const connectionWaiters = new Set<ConnectionWaiter>();

function removeConnectionWaiter(waiter: ConnectionWaiter): void {
    connectionWaiters.delete(waiter);
    if (waiter.signal && waiter.abortHandler) {
        waiter.signal.removeEventListener('abort', waiter.abortHandler);
    }
}

function resolveConnectionWaiters(): void {
    for (const waiter of [...connectionWaiters]) {
        removeConnectionWaiter(waiter);
        waiter.resolve();
    }
}

function rejectConnectionWaiters(error: Error): void {
    for (const waiter of [...connectionWaiters]) {
        removeConnectionWaiter(waiter);
        waiter.reject(error);
    }
}

function connectionWaitAborted(): Error {
    const error = new Error('WebSocket connection wait was superseded');
    error.name = 'AbortError';
    return error;
}

/**
 * Wait for the current or next WS connection without relying on a short
 * polling timeout. The caller owns cancellation so a newer user intent can
 * supersede an older pending action.
 */
export function waitForWsConnection(signal?: AbortSignal): Promise<void> {
    if (isWsConnected()) return Promise.resolve();
    if (signal?.aborted) return Promise.reject(connectionWaitAborted());

    return new Promise<void>((resolve, reject) => {
        const waiter: ConnectionWaiter = { resolve, reject, signal };
        if (signal) {
            waiter.abortHandler = () => {
                removeConnectionWaiter(waiter);
                reject(connectionWaitAborted());
            };
            signal.addEventListener('abort', waiter.abortHandler, { once: true });
        }
        connectionWaiters.add(waiter);

        // Close the subscribe/check race if onopen ran between the first
        // connectivity check and waiter registration.
        if (isWsConnected() && connectionWaiters.has(waiter)) {
            removeConnectionWaiter(waiter);
            resolve();
        }
    });
}

function stopApplicationPing(): void {
    if (applicationPingTimer !== null) {
        clearInterval(applicationPingTimer);
        applicationPingTimer = null;
    }
}

function clearReconnectTimer(): void {
    if (reconnectTimer !== null) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
    }
}

// ==================== 兼容句柄类型（替代 @stomp/stompjs 类型面） ====================

/** 兼容原 IMessage 的最小消息形状（body 为 JSON 文本） */
export interface WsTopicMessage {
    body: string;
}

/** 兼容原 StompSubscription 的最小订阅句柄 */
export interface WsSubscription {
    unsubscribe(): void;
}

/**
 * 兼容原 StompClient 的最小句柄面 — getStompClient() 返回值。
 * subscribe 为兼容占位：原生 WS 无 topic 语义，服务端所有下行统一走主通道
 * 由 dispatch 分发；topic 定向推送（coordinator 等）需后端联动后接通。
 */
export interface WsClientHandle {
    readonly active: boolean;
    readonly connected: boolean;
    publish(params: { destination: string; body: string }): void;
    subscribe(topic: string, handler: (message: WsTopicMessage) => void): WsSubscription;
    deactivate(): void;
}

/** topic 订阅注册表 — 当前无服务端 topic 推送来源，仅保留注册面（见句柄文档） */
const topicSubscriptions = new Map<string, Set<(message: WsTopicMessage) => void>>();
const warnedTopics = new Set<string>();

function createHandle(): WsClientHandle {
    return {
        get active(): boolean {
            return clientActive;
        },
        get connected(): boolean {
            return socket?.readyState === WS_OPEN;
        },
        publish({ destination, body }: { destination: string; body: string }): void {
            let parsed: unknown = {};
            try {
                parsed = body ? JSON.parse(body) : {};
            } catch {
                console.warn('[WS] publish: body is not JSON, sending empty payload for', destination);
            }
            send(destination, parsed as object);
        },
        subscribe(topic: string, handler: (message: WsTopicMessage) => void): WsSubscription {
            if (!warnedTopics.has(topic)) {
                warnedTopics.add(topic);
                console.debug('[WS] subscribe: native WS has no topic push yet, registered only:', topic);
            }
            let handlers = topicSubscriptions.get(topic);
            if (!handlers) {
                handlers = new Set();
                topicSubscriptions.set(topic, handlers);
            }
            handlers.add(handler);
            return {
                unsubscribe: () => {
                    const set = topicSubscriptions.get(topic);
                    set?.delete(handler);
                    if (set && set.size === 0) topicSubscriptions.delete(topic);
                },
            };
        },
        deactivate(): void {
            disconnectStomp();
        },
    };
}

// ==================== URL 构造与连接生命周期 ====================

/**
 * WS URL — 同源 `/ws`（对齐旧 SockJS('/ws') 相对路径语义）：
 * 开发轨经 vite 代理（ws:true）转发到 VITE_API_URL；生产同源直连。
 * authToken 非空时以 token 查询参数携带（原生 WS 无法设置
 * Authorization 头）。服务端仅在兼容窗口内继续接受旧 access_token。
 */
function buildWsUrl(authToken: string): string {
    const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const base = `${scheme}://${window.location.host}/ws`;
    return authToken ? `${base}?token=${encodeURIComponent(authToken)}` : base;
}

/** 底层发送 — 扁平 JSON 信封（顶层 type + payload 字段） */
function sendRaw(payload: Record<string, unknown>): boolean {
    if (socket?.readyState !== WS_OPEN) {
        return false;
    }
    try {
        socket.send(JSON.stringify(payload));
        return true;
    } catch (error) {
        console.error('[WS] send failed', error);
        return false;
    }
}

/**
 * destination → 扁平上行信封映射。
 * `interaction_ack` 的 deliveryGeneration 为服务端必填字段：dispatch.ts 的
 * elicitation 旧路径只发 interactionId，缺省兜底为 1（服务端 handler 实际
 * 忽略该字段，仅反序列化需要）。
 */
function toEnvelope(destination: string, body: unknown): Record<string, unknown> | null {
    const type = DESTINATION_TYPE_MAP[destination];
    if (!type) {
        console.error('[WS] Unknown destination, message dropped:', destination);
        return null;
    }
    const payload: Record<string, unknown> = {
        type,
        ...(typeof body === 'object' && body !== null ? body as Record<string, unknown> : {}),
    };
    if (type === 'interaction_ack' && payload.deliveryGeneration == null) {
        payload.deliveryGeneration = 1;
    }
    return payload;
}

/** 连接成功处理 — 语义对齐旧 onConnect（含 bind→心跳时序） */
function handleOpen(): void {
    reconnectAttempts = 0;
    reconnectStartTime = 0;

    // 重置序列号校验
    resetSequence();

    // ★ 重置会话绑定状态 — 新 WS 连接需要重新 bind_session
    resetBoundSession();

    // 更新连接状态
    useBridgeStore.getState().updateBridgeStatus({ status: 'connected', url: '' });

    // 移除断线警告通知
    useNotificationStore.getState().removeNotification('disconnect-warning');

    // 原生 WS 下行统一走 onmessage（等价旧 /user/queue/messages 订阅，
    // onmessage 在 open 前已挂好），立即 bind 不会丢 session_restored 响应。
    resolveConnectionWaiters();

    // ★ 重连后立即重发 bind_session — 恢复后端 conn↔sessionId 映射
    // 确保正在执行的工具（如 Bash 权限请求）的 push() 能找到连接
    // 注意: 不调用 markSessionBound — 保留 App.tsx handleSubmit 中
    //       bind-session → waitForSessionRestore → addMessage 的安全时序，
    //       防止 session_restored 的 clearMessages() 吞掉用户消息
    stopApplicationPing();
    const activeSessionId = useSessionStore.getState().sessionId;

    // 定义启动心跳的函数 — 确保bind完成后再开始心跳，避免未绑定 pong(bindRequired)
    const startHeartbeat = () => {
        if (!applicationPingTimer) {
            applicationPingTimer = setInterval(() => {
                if (isWsConnected()) sendRaw({ type: 'ping' });
            }, 10_000);
        }
    };

    if (activeSessionId) {
        // 等待bind_session完成后再启动心跳
        void bindSessionAndWait(activeSessionId, payload => {
            const sent = sendRaw({ type: 'bind_session', ...payload });
            if (!sent) throw new Error('bind_session publish failed: socket not open');
        })
        .then(restored => {
            startHeartbeat();
            if (restored) {
                console.info('[WS] Reconnect: re-bound session', activeSessionId);
            } else {
                console.warn('[WS] Reconnect: bind-session was not restored');
            }
        })
        .catch(() => {
            // bind失败也启动心跳（支持容错，后端会返回bindRequired）
            startHeartbeat();
            console.warn('[WS] Reconnect: bind-session failed, heartbeat started with fallback');
        });
    } else {
        // 没有活跃会话时也启动心跳
        startHeartbeat();
    }
}

/** 连接关闭处理 — 语义对齐旧 onWebSocketClose（退避重连 + 10min 总超时） */
function handleClose(): void {
    stopApplicationPing();

    // 主动断开（deactivate）不重连
    if (!clientActive) {
        return;
    }

    if (reconnectStartTime === 0) {
        reconnectStartTime = Date.now();
    }

    reconnectAttempts++;

    // 更新连接状态
    if (reconnectAttempts === 1) {
        useBridgeStore.getState().updateBridgeStatus({ status: 'disconnected', url: '' });
        useNotificationStore.getState().addNotification({
            key: 'disconnect-warning',
            level: 'warning',
            message: '连接已断开，正在尝试重连...',
            timeout: 0,  // 不自动消失
        });
    } else {
        useBridgeStore.getState().updateBridgeStatus({ status: 'reconnecting', url: '' });
    }

    // 重连超时检测 (10min)
    if (Date.now() - reconnectStartTime > RECONNECT_TIMEOUT) {
        rejectConnectionWaiters(
            new Error('WebSocket 重连超时，请刷新页面后重试'),
        );
        clientActive = false;
        useBridgeStore.getState().updateBridgeStatus({ status: 'disconnected', url: '' });
        useNotificationStore.getState().removeNotification('disconnect-warning');
        useNotificationStore.getState().addNotification({
            key: 'reconnect-failed',
            level: 'error',
            message: '连接已断开，请刷新页面重试',
            timeout: 0,
        });
        return;
    }

    // 指数退避重连延迟
    const delay = Math.min(
        RECONNECT_DELAY_INITIAL * Math.pow(2, reconnectAttempts - 1),
        RECONNECT_DELAY_MAX
    );
    clearReconnectTimer();
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        openSocket();
    }, delay);
}

/** 建立底层 WebSocket 并挂接生命周期回调 */
function openSocket(): void {
    if (!clientActive) return;

    let ws: WebSocket;
    try {
        ws = new WebSocket(buildWsUrl(currentAuthToken));
    } catch (error) {
        console.error('[WS] WebSocket construction failed:', error);
        handleClose();
        return;
    }
    socket = ws;

    ws.onopen = () => {
        if (socket !== ws) return;
        handleOpen();
    };
    ws.onmessage = (event: MessageEvent) => {
        if (socket !== ws) return;
        const data = parseMessage(typeof event.data === 'string' ? event.data : '');
        if (data) {
            dispatch(data);
        }
    };
    ws.onclose = () => {
        if (socket !== ws) return;
        socket = null;
        handleClose();
    };
    ws.onerror = () => {
        // onerror 后必然触发 onclose，重连逻辑统一在 handleClose
        console.debug('[WS] socket error (close will follow)');
    };
}

/**
 * 创建并激活原生 WS 客户端连接（保留旧函数名与签名，调用方零改动）
 */
export function createStompClient(_sessionId: string, authToken: string): WsClientHandle {
    // 如果已有连接，先断开（不触发旧 socket 的重连）
    clearReconnectTimer();
    if (socket) {
        const old = socket;
        socket = null;
        try { old.close(); } catch { /* ignore */ }
    }

    clientActive = true;
    currentAuthToken = authToken;
    reconnectAttempts = 0;
    reconnectStartTime = 0;

    wsHandle = createHandle();
    openSocket();
    return wsHandle;
}

/**
 * 断开 WS 连接
 */
export function disconnectStomp(): void {
    stopApplicationPing();
    clearReconnectTimer();
    rejectConnectionWaiters(new Error('WebSocket 连接已关闭'));
    clientActive = false;
    if (socket) {
        const old = socket;
        socket = null;
        try { old.close(); } catch { /* ignore */ }
    }
    wsHandle = null;
}

/**
 * 获取当前客户端句柄（原 getStompClient — 返回兼容最小句柄面）
 */
export function getStompClient(): WsClientHandle | null {
    return wsHandle;
}

/**
 * 检查客户端激活状态（对齐旧 stompClient.active 语义：重连中仍为 true）
 */
export function isConnected(): boolean {
    return clientActive;
}

// ==================== 便捷发送方法 — destination 签名保留 ====================

/** 发送消息（destination → 顶层 type 映射后走扁平信封） */
export function send(destination: string, body: object): void {
    if (socket?.readyState !== WS_OPEN) {
        console.warn('[WS] Cannot send: not connected');
        return;
    }
    const envelope = toEnvelope(destination, body);
    if (envelope) sendRaw(envelope);
}

/** #1 发送用户消息 → user_message */
export function sendUserMessage(text: string, attachments?: Attachment[], references?: Array<{ type: string; path: string }>): void {
    send('/app/chat', { text, attachments, references });
}

/** 向当前运行中的根任务追加指令，不中断当前调用。 */
export function sendRunInput(requestId: string, text: string): boolean {
    return sendToServer('/app/run-input', { requestId, text });
}

/** #3 发送中断 → interrupt */
export function sendInterrupt(): void {
    send('/app/interrupt', {});
}

/** #4 切换模型 → set_model */
export function sendSetModel(model: string): void {
    send('/app/model', { model });
}

/** #5 切换权限模式 → set_permission_mode */
export function sendSetPermissionMode(mode: string): boolean {
    return sendToServer('/app/permission-mode', { mode });
}

/** #6 Slash 命令 → slash_command */
export function sendSlashCommand(command: string, args: string): boolean {
    return sendToServer('/app/command', { command, args });
}

/** #7 MCP 操作 → mcp_operation */
export function sendMcpOperation(operation: string, serverId: string, config?: object): void {
    send('/app/mcp', { operation, serverId, config });
}

/** #8 回退文件 → rewind_files */
export function sendRewindFiles(messageId: string, filePaths: string[]): void {
    send('/app/rewind', { messageId, filePaths });
}

/** #10 心跳探测 → ping */
export function sendPing(): void {
    send('/app/ping', {});
}

// ==================== 兼容导出 — 统一 useWebSocket 迁移 ====================

/**
 * sendToServer — 兼容原 useWebSocket.ts 的模块级发送函数
 * 返回 boolean 表示是否发送成功
 */
export function sendToServer(destination: string, body: unknown): boolean {
    if (socket?.readyState !== WS_OPEN) {
        console.warn('[WS] sendToServer: not connected');
        return false;
    }
    const envelope = toEnvelope(destination, body);
    if (!envelope) return false;
    return sendRaw(envelope);
}

/**
 * isWsConnected — 兼容原 useWebSocket.ts 的连接状态检查
 */
export function isWsConnected(): boolean {
    return socket?.readyState === WS_OPEN;
}
