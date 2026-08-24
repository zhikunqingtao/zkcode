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
}

function attachCompletedToolResults(messages: Message[]): Message[] {
    const results = new Map<string, ToolResult>();
    for (const message of messages) {
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
    if (results.size === 0) return messages;
    return messages.map(message => {
        if (message.type !== 'assistant') return message;
        let changed = false;
        const content = message.content.map(block => {
            if (block.type !== 'tool_use') return block;
            const result = results.get(block.toolUseId);
            if (!result) return block;
            changed = true;
            return { ...block, result };
        });
        return changed ? { ...message, content } : message;
    });
}

export interface MessageStoreState {
    // 状态
    messages: Message[];
    streamingMessageId: string | null;
    streamingContent: string;
    thinkingContent: string;
    activeToolCalls: Map<string, ToolCallState>;
    tokenBudgetState: TokenBudgetState | null;
    tokenWarning: TokenWarningPayload | null;

    // Actions
    addMessage: (msg: Message) => void;
    appendStreamDelta: (delta: string) => void;
    appendThinkingDelta: (delta: string) => void;
    startToolCall: (toolUseId: string, toolName: string, input: unknown) => void;
    updateToolCallInput: (toolUseId: string, input: unknown) => void;
    updateToolCallProgress: (toolUseId: string, progress: string) => void;
    completeToolCall: (toolUseId: string, result: ToolResult) => void;
    replaceActiveToolCalls: (calls: RecoveredToolCall[]) => void;
    restoreSessionSnapshot: (messages: Message[], calls: RecoveredToolCall[]) => void;
    reconcileCommittedRun: (replaceAfterMessageId: string | null, messages: Message[]) => boolean;
    finalizeAssistantSegment: () => void;
    finalizeStream: (usage: Usage) => void;
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
        activeToolCalls: new Map(),
        tokenBudgetState: null,
        tokenWarning: null,

        addMessage: (msg) => set(d => {
            const existing = d.messages.findIndex(item => item.uuid === msg.uuid);
            if (existing >= 0) d.messages[existing] = msg;
            else d.messages.push(msg);
        }),
        appendStreamDelta: (delta) => set(d => {
            // 首次收到 stream_delta 时，创建占位 assistant 消息
            if (!d.streamingMessageId) {
                const msgId = generateUUID();
                d.streamingMessageId = msgId;
                d.messages.push({
                    uuid: msgId,
                    type: 'assistant',
                    content: [{ type: 'text', text: '' }],
                    timestamp: Date.now(),
                } as Message);
            }
            d.streamingContent += delta;
        }),
        appendThinkingDelta: (delta) => set(d => {
            // 首次收到 thinking_delta 时，也创建占位 assistant 消息
            if (!d.streamingMessageId) {
                const msgId = generateUUID();
                d.streamingMessageId = msgId;
                d.messages.push({
                    uuid: msgId,
                    type: 'assistant',
                    content: [],
                    timestamp: Date.now(),
                    stopReason: '',
                    usage: { inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
                } as unknown as Message);
            }
            d.thinkingContent += delta;
            // 同步更新 streaming message 中的 thinking block
            const msg = d.messages.find(m => m.uuid === d.streamingMessageId);
            if (msg && msg.type === 'assistant' && Array.isArray((msg as any).content)) {
                const content = (msg as any).content;
                const thinkingBlock = content.find((b: any) => b.type === 'thinking' && !b.completed);
                if (thinkingBlock) {
                    thinkingBlock.thinking = d.thinkingContent;
                } else {
                    content.unshift({ type: 'thinking', thinking: d.thinkingContent, completed: false });
                }
            }
        }),
        startToolCall: (id, name, input) => set(d => {
            d.activeToolCalls.set(id, {
                toolName: name, input, status: 'running', startTime: Date.now(),
            });
        }),
        updateToolCallInput: (id, input) => set(d => {
            const tc = d.activeToolCalls.get(id);
            if (tc) {
                tc.input = input;
            }
        }),
        updateToolCallProgress: (id, progress) => set(d => {
            const tc = d.activeToolCalls.get(id);
            if (tc) {
                tc.progress = progress;
                if (!tc.progressHistory) tc.progressHistory = [];
                tc.progressHistory.push(progress);
            }
        }),
        completeToolCall: (id, result) => set(d => {
            const tc = d.activeToolCalls.get(id);
            if (tc) {
                tc.status = result.isError ? 'error' : 'completed';
                tc.result = result;
                tc.duration = Date.now() - tc.startTime;
            }
        }),
        replaceActiveToolCalls: (calls) => set(d => {
            d.activeToolCalls.clear();
            calls.forEach(call => d.activeToolCalls.set(call.toolUseId, {
                toolName: call.toolName || 'Tool',
                input: call.input ?? {},
                status: 'running',
                startTime: call.startedAt ?? Date.now(),
            }));
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
                d.activeToolCalls.clear();
                calls.forEach(call => d.activeToolCalls.set(call.toolUseId, {
                    toolName: call.toolName || 'Tool',
                    input: call.input ?? {},
                    status: 'running',
                    startTime: call.startedAt ?? Date.now(),
                }));
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
        finalizeAssistantSegment: () => set(d => {
            flushStreamingBuffer();
            const externalContent = streamingStore.clear();
            const combinedContent = d.streamingContent + externalContent;
            if (d.streamingMessageId) {
                const msg = d.messages.find(m => m.uuid === d.streamingMessageId);
                if (msg && msg.type === 'assistant') {
                    const content: any[] = [];
                    if (d.thinkingContent) {
                        content.push({ type: 'thinking' as const, thinking: d.thinkingContent, completed: true });
                    }
                    if (combinedContent) {
                        content.push({ type: 'text' as const, text: combinedContent });
                    }
                    (msg as { content: unknown }).content = content;
                }
            }
            d.streamingMessageId = null;
            d.streamingContent = '';
            d.thinkingContent = '';
        }),
        finalizeStream: (_usage) => set(d => {
            // 先刷新 streamingStore 中的剩余缓冲
            flushStreamingBuffer();
            const externalContent = streamingStore.clear();

            // 将累积的流式内容保存到 messages 中的 assistant 消息
            const combinedContent = d.streamingContent + externalContent;
            if (d.streamingMessageId) {
                const msg = d.messages.find(m => m.uuid === d.streamingMessageId);
                if (msg && 'content' in msg && msg.type === 'assistant') {
                    const content: any[] = [];
                    // 保留 thinking block (标记为 completed)
                    if (d.thinkingContent) {
                        content.push({ type: 'thinking' as const, thinking: d.thinkingContent, completed: true });
                    }
                    // 文本内容
                    if (combinedContent) {
                        content.push({ type: 'text' as const, text: combinedContent });
                    }
                    (msg as { content: unknown }).content = content;
                }
            }
            d.streamingMessageId = null;
            d.streamingContent = '';
            d.thinkingContent = '';
        }),
        clearMessages: () => {
            flushStreamingBuffer();
            streamingStore.clear();
            set(d => {
                d.messages = [];
                d.streamingMessageId = null;
                d.streamingContent = '';
                d.thinkingContent = '';
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
