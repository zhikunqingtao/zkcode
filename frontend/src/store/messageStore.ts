/**
 * MessageStore — 消息状态管理
 * SPEC: §8.3 Store #2
 * 持久化: 否 (从后端 session_restored 加载)
 */

import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { subscribeWithSelector } from 'zustand/middleware';
import type { Message, ToolResult, ToolCallState, Usage, TokenWarningPayload } from '@/types';
import { streamingStore, flushStreamingBuffer } from '@/hooks/useStreamingText';
import { generateUUID } from '@/utils/uuid';

export interface TokenBudgetState {
    pct: number;
    currentTokens: number;
    budgetTokens: number;
    visible: boolean;
}

export interface RecoveredToolCall {
    toolUseId: string;
    toolName: string;
    input: unknown;
    startedAt?: number;
    runtimePartitionKey?: string;
    phase?: 'preparing' | 'running';
}

export interface StreamingPartitionState {
    key: string;
    messageId: string;
    content: string;
    thinkingContent: string;
}

export interface FinalizeStreamOptions {
    /**
     * A terminal Agent event is the last boundary for its runtime partition.
     * Any invocation still shown as preparing/running at that point has missed
     * its terminal frame and must become a tombstone instead of a ghost tool.
     */
    closeActiveTools?: boolean;
    orphanedToolMessage?: string;
}

export const ROOT_RUNTIME_PARTITION = 'root';

function toolCallKey(partitionKey: string, toolUseId: string): string {
    return partitionKey === ROOT_RUNTIME_PARTITION
        ? toolUseId : `${partitionKey}\u0000${toolUseId}`;
}

function isTerminalToolCall(status: ToolCallState['status']): boolean {
    return status === 'completed' || status === 'error';
}

/**
 * Child results are durable user-role messages because they must enter the
 * parent's model transcript.  Their tagged payload is runtime input, not a
 * human chat bubble; the task tree renders the same result explicitly.
 */
function isInternalTaskResultMessage(message: Message): boolean {
    if (message.type !== 'user' || message.content.length !== 1) return false;
    const [block] = message.content;
    if (block.type !== 'text') return false;
    const text = block.text.trim();
    return text.startsWith('<task-result ') && text.endsWith('</task-result>');
}

/**
 * 将持久化的 user/tool_result 投影回对应 tool_use，并从用户可见历史中
 * 移除已被消费的内部 block。这个投影保留同一 user message 中真正的文本/图片。
 */
function attachCompletedToolResults(messages: Message[]): Message[] {
    const visibleMessages = messages.filter(message => !isInternalTaskResultMessage(message));
    const results = new Map<string, ToolResult>();
    for (const message of visibleMessages) {
        if (message.type !== 'user') continue;
        for (const block of message.content) {
            if (block.type !== 'tool_result') continue;
            results.set(block.toolUseId, {
                content: block.content,
                isError: block.isError,
                metadata: block.metadata,
            });
        }
    }
    if (results.size === 0) return visibleMessages;
    const projected: Message[] = [];
    for (const message of visibleMessages) {
        if (message.type === 'user') {
            const content = message.content.filter(block => block.type !== 'tool_result');
            if (content.length > 0) projected.push({ ...message, content });
            continue;
        }
        if (message.type !== 'assistant') {
            projected.push(message);
            continue;
        }
        let changed = false;
        const content = message.content.map(block => {
            if (block.type !== 'tool_use') return block;
            const result = results.get(block.toolUseId);
            if (!result) return block;
            changed = true;
            return { ...block, result };
        });
        projected.push(changed ? { ...message, content } : message);
    }
    return projected;
}

export interface MessageStoreState {
    // 状态
    messages: Message[];
    streamingMessageId: string | null;
    streamingContent: string;
    thinkingContent: string;
    /** source Run/Task 分区，防止并发 Agent 增量拼成同一条消息。 */
    streamingPartitions: Map<string, StreamingPartitionState>;
    /** 消息到运行时分区的稳定映射，在流结束后仍用于工具归属。 */
    messagePartitionKeys: Map<string, string>;
    activeToolCalls: Map<string, ToolCallState>;
    tokenBudgetState: TokenBudgetState | null;
    tokenWarning: TokenWarningPayload | null;

    // Actions
    addMessage: (msg: Message) => void;
    appendStreamDelta: (delta: string, partitionKey?: string) => void;
    appendThinkingDelta: (delta: string, partitionKey?: string) => void;
    startToolCall: (toolUseId: string, toolName: string, input: unknown, partitionKey?: string) => void;
    updateToolCallInput: (toolUseId: string, input: unknown, partitionKey?: string) => void;
    updateToolCallProgress: (toolUseId: string, progress: string, partitionKey?: string) => void;
    completeToolCall: (toolUseId: string, result: ToolResult, partitionKey?: string) => void;
    replaceActiveToolCalls: (calls: RecoveredToolCall[]) => void;
    restoreSessionSnapshot: (messages: Message[], calls: RecoveredToolCall[]) => void;
    reconcileCommittedRun: (replaceAfterMessageId: string | null, messages: Message[]) => boolean;
    finalizeAssistantSegment: (partitionKey?: string) => void;
    finalizeStream: (
        usage: Usage,
        partitionKey?: string,
        options?: FinalizeStreamOptions,
    ) => void;
    clearMessages: () => void;
    rewindToMessage: (messageId: string) => void;
    setTokenBudgetState: (state: TokenBudgetState | null) => void;
    clearTokenBudgetState: () => void;
    setTokenWarning: (warning: TokenWarningPayload | null) => void;
    clearTokenWarning: () => void;
}

export const useMessageStore = create<MessageStoreState>()(
    subscribeWithSelector(immer((set) => ({
        messages: [],
        streamingMessageId: null,
        streamingContent: '',
        thinkingContent: '',
        streamingPartitions: new Map(),
        messagePartitionKeys: new Map(),
        activeToolCalls: new Map(),
        tokenBudgetState: null,
        tokenWarning: null,

        addMessage: (msg) => set(d => {
            const existing = d.messages.findIndex(item => item.uuid === msg.uuid);
            if (existing >= 0) d.messages[existing] = msg;
            else d.messages.push(msg);
        }),
        appendStreamDelta: (delta, partitionKey = ROOT_RUNTIME_PARTITION) => set(d => {
            let partition = d.streamingPartitions.get(partitionKey);
            if (!partition) {
                const msgId = generateUUID();
                d.messages.push({
                    uuid: msgId,
                    type: 'assistant',
                    content: [{ type: 'text', text: '' }],
                    timestamp: Date.now(),
                } as Message);
                partition = { key: partitionKey, messageId: msgId, content: '', thinkingContent: '' };
                d.streamingPartitions.set(partitionKey, partition);
                d.messagePartitionKeys.set(msgId, partitionKey);
            }
            partition.content += delta;
            if (partitionKey === ROOT_RUNTIME_PARTITION) {
                d.streamingMessageId = partition.messageId;
                d.streamingContent = partition.content;
            }
        }),
        appendThinkingDelta: (delta, partitionKey = ROOT_RUNTIME_PARTITION) => set(d => {
            let partition = d.streamingPartitions.get(partitionKey);
            if (!partition) {
                const msgId = generateUUID();
                d.messages.push({
                    uuid: msgId,
                    type: 'assistant',
                    content: [],
                    timestamp: Date.now(),
                    stopReason: '',
                    usage: { inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
                } as unknown as Message);
                partition = { key: partitionKey, messageId: msgId, content: '', thinkingContent: '' };
                d.streamingPartitions.set(partitionKey, partition);
                d.messagePartitionKeys.set(msgId, partitionKey);
            }
            partition.thinkingContent += delta;
            if (partitionKey === ROOT_RUNTIME_PARTITION) {
                d.streamingMessageId = partition.messageId;
                d.thinkingContent = partition.thinkingContent;
            }
            const msg = d.messages.find(m => m.uuid === partition?.messageId);
            if (msg && msg.type === 'assistant' && Array.isArray((msg as any).content)) {
                const content = (msg as any).content;
                const thinkingBlock = content.find((b: any) => b.type === 'thinking' && !b.completed);
                if (thinkingBlock) {
                    thinkingBlock.thinking = partition.thinkingContent;
                } else {
                    content.unshift({ type: 'thinking', thinking: partition.thinkingContent, completed: false });
                }
            }
        }),
        startToolCall: (id, name, input, partitionKey = ROOT_RUNTIME_PARTITION) => set(d => {
            const key = toolCallKey(partitionKey, id);
            const existing = d.activeToolCalls.get(key);
            // WS replay is at-least-once and a delayed `tool_use_start` must never
            // move an immutable terminal invocation back to preparing (ghost Running).
            if (existing && isTerminalToolCall(existing.status)) return;
            if (!d.streamingPartitions.has(partitionKey)) {
                const messageId = generateUUID();
                d.messages.push({
                    uuid: messageId,
                    type: 'assistant',
                    content: [],
                    timestamp: Date.now(),
                    stopReason: '',
                    usage: { inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
                } as Message);
                d.streamingPartitions.set(partitionKey, {
                    key: partitionKey,
                    messageId,
                    content: '',
                    thinkingContent: '',
                });
                d.messagePartitionKeys.set(messageId, partitionKey);
                if (partitionKey === ROOT_RUNTIME_PARTITION) d.streamingMessageId = messageId;
            }
            d.activeToolCalls.set(key, {
                toolUseId: id,
                runtimePartitionKey: partitionKey,
                toolName: name,
                input,
                status: existing?.status ?? 'preparing',
                startTime: existing?.startTime ?? Date.now(),
            });
        }),
        updateToolCallInput: (id, input, partitionKey = ROOT_RUNTIME_PARTITION) => set(d => {
            const key = toolCallKey(partitionKey, id);
            const existing = d.activeToolCalls.get(key);
            // A late input frame cannot reopen a completed invocation.
            if (existing && isTerminalToolCall(existing.status)) return;
            const tc = existing ?? {
                toolUseId: id,
                runtimePartitionKey: partitionKey,
                toolName: 'Tool',
                input: {},
                status: 'preparing' as const,
                startTime: Date.now(),
            };
            tc.input = input;
            tc.status = 'running';
            tc.startTime = Date.now();
            d.activeToolCalls.set(key, tc);
        }),
        updateToolCallProgress: (id, progress, partitionKey = ROOT_RUNTIME_PARTITION) => set(d => {
            const tc = d.activeToolCalls.get(toolCallKey(partitionKey, id));
            if (tc && !isTerminalToolCall(tc.status)) {
                tc.progress = progress;
                if (!tc.progressHistory) tc.progressHistory = [];
                tc.progressHistory.push(progress);
            }
        }),
        completeToolCall: (id, result, partitionKey = ROOT_RUNTIME_PARTITION) => set(d => {
            const key = toolCallKey(partitionKey, id);
            const existing = d.activeToolCalls.get(key);
            // Tool invocation terminal state is immutable in SQLite; mirror that
            // invariant in the live projection when duplicate/late frames arrive.
            if (existing && isTerminalToolCall(existing.status)) return;
            const tc = existing ?? {
                toolUseId: id,
                runtimePartitionKey: partitionKey,
                toolName: 'Tool',
                input: {},
                status: 'preparing' as const,
                startTime: Date.now(),
            };
            tc.status = result.isError ? 'error' : 'completed';
            tc.result = result;
            tc.duration = Date.now() - tc.startTime;
            d.activeToolCalls.set(key, tc);
        }),
        replaceActiveToolCalls: (calls) => set(d => {
            d.activeToolCalls.clear();
            calls.forEach(call => {
                const partitionKey = call.runtimePartitionKey ?? ROOT_RUNTIME_PARTITION;
                d.activeToolCalls.set(toolCallKey(partitionKey, call.toolUseId), {
                toolUseId: call.toolUseId,
                runtimePartitionKey: partitionKey,
                toolName: call.toolName || 'Tool',
                input: call.input ?? {},
                status: call.phase ?? 'running',
                startTime: call.startedAt ?? Date.now(),
                });
            });
        }),
        restoreSessionSnapshot: (messages, calls) => {
            flushStreamingBuffer();
            streamingStore.clear();
            const projectedMessages = attachCompletedToolResults(messages);
            set(d => {
                d.messages = projectedMessages;
                d.streamingMessageId = null;
                d.streamingContent = '';
                d.thinkingContent = '';
                d.streamingPartitions.clear();
                d.messagePartitionKeys.clear();
                d.activeToolCalls.clear();
                calls.forEach(call => {
                    const partitionKey = call.runtimePartitionKey ?? ROOT_RUNTIME_PARTITION;
                    if (!d.streamingPartitions.has(partitionKey)) {
                        const messageId = generateUUID();
                        d.messages.push({
                            uuid: messageId,
                            type: 'assistant',
                            content: [],
                            timestamp: Date.now(),
                            stopReason: '',
                            usage: { inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
                        } as Message);
                        d.streamingPartitions.set(partitionKey, {
                            key: partitionKey,
                            messageId,
                            content: '',
                            thinkingContent: '',
                        });
                        d.messagePartitionKeys.set(messageId, partitionKey);
                        if (partitionKey === ROOT_RUNTIME_PARTITION) {
                            d.streamingMessageId = messageId;
                        }
                    }
                    d.activeToolCalls.set(toolCallKey(partitionKey, call.toolUseId), {
                    toolUseId: call.toolUseId,
                    runtimePartitionKey: partitionKey,
                    toolName: call.toolName || 'Tool',
                    input: call.input ?? {},
                    status: call.phase ?? 'running',
                    startTime: call.startedAt ?? Date.now(),
                    });
                });
                d.tokenBudgetState = null;
                d.tokenWarning = null;
            });
        },
        reconcileCommittedRun: (replaceAfterMessageId, messages) => {
            if (messages.length === 0) return false;
            const incomingIds = new Set<string>();
            for (const message of messages) {
                if (!message.uuid || incomingIds.has(message.uuid)) return false;
                incomingIds.add(message.uuid);
            }
            const projectedMessages = attachCompletedToolResults(messages);
            let reconciled = false;
            set(d => {
                const keepCount = replaceAfterMessageId === null
                    ? 0
                    : d.messages.findIndex(message => message.uuid === replaceAfterMessageId) + 1;
                if (replaceAfterMessageId !== null && keepCount === 0) return;
                for (let i = 0; i < keepCount; i++) {
                    if (incomingIds.has(d.messages[i].uuid)) return;
                }
                d.messages.splice(keepCount, d.messages.length - keepCount, ...projectedMessages);
                d.streamingMessageId = null;
                d.streamingContent = '';
                d.thinkingContent = '';
                d.streamingPartitions.clear();
                d.messagePartitionKeys.clear();
                d.activeToolCalls.clear();
                d.tokenBudgetState = null;
                d.tokenWarning = null;
                reconciled = true;
            });
            if (reconciled) {
                flushStreamingBuffer();
                streamingStore.clear();
            }
            return reconciled;
        },
        finalizeAssistantSegment: (partitionKey = ROOT_RUNTIME_PARTITION) => set(d => {
            const partition = d.streamingPartitions.get(partitionKey);
            if (!partition) return;
            const externalContent = partitionKey === ROOT_RUNTIME_PARTITION
                ? (flushStreamingBuffer(), streamingStore.clear()) : '';
            const combinedContent = partition.content + externalContent;
            if (partition.messageId) {
                const msg = d.messages.find(m => m.uuid === partition.messageId);
                if (msg && msg.type === 'assistant') {
                    const content: any[] = [];
                    if (partition.thinkingContent) {
                        content.push({ type: 'thinking' as const, thinking: partition.thinkingContent, completed: true });
                    }
                    if (combinedContent) {
                        content.push({ type: 'text' as const, text: combinedContent });
                    }
                    (msg as { content: unknown }).content = content;
                }
            }
            d.streamingPartitions.delete(partitionKey);
            if (partitionKey === ROOT_RUNTIME_PARTITION) {
                d.streamingMessageId = null;
                d.streamingContent = '';
                d.thinkingContent = '';
            }
        }),
        finalizeStream: (
            _usage,
            partitionKey = ROOT_RUNTIME_PARTITION,
            options,
        ) => set(d => {
            const partition = d.streamingPartitions.get(partitionKey);
            if (partition) {
                const externalContent = partitionKey === ROOT_RUNTIME_PARTITION
                    ? (flushStreamingBuffer(), streamingStore.clear()) : '';
                const combinedContent = partition.content + externalContent;
                if (partition.messageId) {
                    const msg = d.messages.find(m => m.uuid === partition.messageId);
                    if (msg && 'content' in msg && msg.type === 'assistant') {
                        const content: any[] = [];
                        if (partition.thinkingContent) {
                            content.push({ type: 'thinking' as const, thinking: partition.thinkingContent, completed: true });
                        }
                        // 文本内容
                        if (combinedContent) {
                            content.push({ type: 'text' as const, text: combinedContent });
                        }
                        (msg as { content: unknown }).content = content;
                    }
                }
                d.streamingPartitions.delete(partitionKey);
            }

            if (options?.closeActiveTools) {
                const terminalMessage = options.orphanedToolMessage
                    ?? 'Agent ended before this tool reported a terminal result.';
                d.activeToolCalls.forEach(toolCall => {
                    const callPartition = toolCall.runtimePartitionKey
                        ?? ROOT_RUNTIME_PARTITION;
                    if (callPartition !== partitionKey || isTerminalToolCall(toolCall.status)) {
                        return;
                    }
                    toolCall.status = 'error';
                    toolCall.result = {
                        content: terminalMessage,
                        isError: true,
                        metadata: { reason: 'agent_partition_terminal' },
                    };
                    toolCall.duration = Math.max(0, Date.now() - toolCall.startTime);
                });
            }

            if (partitionKey === ROOT_RUNTIME_PARTITION) {
                d.streamingMessageId = null;
                d.streamingContent = '';
                d.thinkingContent = '';
            }
        }),
        clearMessages: () => {
            flushStreamingBuffer();
            streamingStore.clear();
            set(d => {
                d.messages = [];
                d.streamingMessageId = null;
                d.streamingContent = '';
                d.thinkingContent = '';
                d.streamingPartitions.clear();
                d.messagePartitionKeys.clear();
                d.activeToolCalls.clear();
                d.tokenBudgetState = null;
                d.tokenWarning = null;
            });
        },
        rewindToMessage: (messageId) => set(d => {
            const idx = d.messages.findIndex(m => m.uuid === messageId);
            if (idx >= 0) d.messages.splice(idx + 1);
        }),
        setTokenBudgetState: (state) => set(d => { d.tokenBudgetState = state; }),
        clearTokenBudgetState: () => set(d => { d.tokenBudgetState = null; }),
        setTokenWarning: (warning) => set((draft) => { draft.tokenWarning = warning; }),
        clearTokenWarning: () => set((draft) => { draft.tokenWarning = null; }),
    })))
);
