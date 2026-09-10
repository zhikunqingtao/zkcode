import { describe, test, expect, vi, beforeEach } from 'vitest';
import { useMessageStore } from '@/store/messageStore';
import { useSessionStore } from '@/store/sessionStore';
import { usePermissionStore } from '@/store/permissionStore';
import { useNotificationStore } from '@/store/notificationStore';
import { useCostStore } from '@/store/costStore';
import { useTaskStore } from '@/store/taskStore';
import { useCoordinatorStore } from '@/store/coordinatorStore';
import { bindSessionAndWait, dispatch, resetBoundSession } from '@/api/dispatch';
import { runtimeEnvelope } from '@/test/runtimeEnvelope';

beforeEach(() => {
    resetBoundSession();
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, json: async () => [] }));
    // Reset stores between tests
    useMessageStore.setState({
        messages: [],
        streamingMessageId: null,
        streamingContent: '',
        thinkingContent: '',
        streamingPartitions: new Map(),
        messagePartitionKeys: new Map(),
        activeToolCalls: new Map(),
    });
    useSessionStore.setState({
        sessionId: null,
        model: null,
        status: 'idle',
        turnCount: 0,
        isAborted: false,
    });
    usePermissionStore.setState({
        pendingPermissions: [],
        permissionMode: 'default',
    });
    useTaskStore.getState().clearTasks();
    useCoordinatorStore.getState().clearAll();
});

describe('dispatch 消息分发', () => {
    test('stream_delta → appendStreamDelta (external store)', () => {
        // stream_delta now goes to external streaming store, not messageStore
        // Verify it doesn't throw
        expect(() => {
            dispatch({
            ...runtimeEnvelope(), type: 'stream_delta', delta: 'hello', messageId: 'msg-1', ts: 1 } as never);
        }).not.toThrow();
    });

    test('v4 eventId is idempotent and child streams stay out of the root conversation', () => {
        const eventA = runtimeEnvelope({
            taskId: 'task-root', runId: 'run-root',
            sourceTaskId: 'task-root', sourceRunId: 'run-root',
        });
        dispatch({ ...eventA, type: 'stream_delta', delta: 'A', messageId: 'a' } as never);
        dispatch({ ...eventA, type: 'stream_delta', delta: 'duplicate', messageId: 'a' } as never);
        dispatch({
            ...runtimeEnvelope({
                taskId: 'task-root', runId: 'run-root',
                sourceTaskId: 'task-child', sourceRunId: 'run-child',
            }),
            type: 'stream_delta', delta: 'B', messageId: 'b',
        } as never);

        const partitions = useMessageStore.getState().streamingPartitions;
        expect(partitions.get('sourceRun:run-root')?.content).toBe('A');
        expect(partitions.has('sourceRun:run-child')).toBe(false);
    });

    test('late tool frames cannot reopen a terminal root invocation', () => {
        const actor = {
            taskId: 'task-root', runId: 'run-root',
            sourceTaskId: 'task-root', sourceRunId: 'run-root', toolUseId: 'tool-1',
        };
        dispatch({
            ...runtimeEnvelope(actor), type: 'tool_result', toolUseId: 'tool-1',
            content: 'done', isError: false,
        } as never);
        dispatch({
            ...runtimeEnvelope(actor), type: 'tool_use_start', toolUseId: 'tool-1',
            toolName: 'Bash', input: {},
        } as never);
        dispatch({
            ...runtimeEnvelope(actor), type: 'tool_use_input', toolUseId: 'tool-1',
            toolName: 'Bash', input: { command: 'pwd' },
        } as never);

        const tool = useMessageStore.getState().activeToolCalls
            .get('sourceRun:run-root\u0000tool-1');
        expect(tool?.status).toBe('completed');
        expect(tool?.result?.content).toBe('done');
        expect(useMessageStore.getState().streamingPartitions.size).toBe(0);
    });

    test('drops a non-v4 event before it can mutate stores', () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => undefined);
        dispatch({ type: 'stream_delta', delta: 'legacy', messageId: 'legacy' } as never);
        expect(useMessageStore.getState().streamingPartitions.size).toBe(0);
        expect(consoleError).toHaveBeenCalled();
        consoleError.mockRestore();
    });

    test('session_restored → clearMessages + addMessage + resumeSession', async () => {
        let bindRequestId = '';
        let bindingEpoch = 0;
        const bound = bindSessionAndWait('s1', payload => {
            bindRequestId = payload.bindRequestId;
            bindingEpoch = payload.bindingEpoch;
        });
        dispatch({
            ...runtimeEnvelope(),
            type: 'session_restored', ts: 1, bindRequestId, protocolVersion: 4,
            bindingEpoch,
            messages: [{ type: 'user', uuid: '1', timestamp: 1, content: [{ type: 'text', text: 'hi' }] }],
            metadata: { sessionId: 's1', model: 'gpt-4o', permissionMode: 'AUTO_APPROVE', status: 'idle' },
        } as never);
        await expect(bound).resolves.toBe(true);
        expect(useMessageStore.getState().messages).toHaveLength(1);
        expect(useSessionStore.getState().model).toBe('gpt-4o');
        expect(usePermissionStore.getState().permissionMode).toBe('auto_approve');
    });

    test('session restore projects only root active tools into the conversation', async () => {
        let bindRequestId = '';
        let bindingEpoch = 0;
        const bound = bindSessionAndWait('s1', payload => {
            bindRequestId = payload.bindRequestId;
            bindingEpoch = payload.bindingEpoch;
        });
        const rootContext = runtimeEnvelope({
            sessionId: 's1', taskId: 'task-root', runId: 'run-root',
            sourceTaskId: 'task-root', sourceRunId: 'run-root', toolUseId: 'root-tool',
        }).eventContext;
        const childContext = runtimeEnvelope({
            sessionId: 's1', taskId: 'task-root', runId: 'run-root',
            sourceTaskId: 'task-child', sourceRunId: 'run-child', toolUseId: 'child-tool',
        }).eventContext;

        dispatch({
            ...runtimeEnvelope(),
            type: 'session_restored', bindRequestId, protocolVersion: 4, bindingEpoch,
            messages: [],
            metadata: { sessionId: 's1', model: 'gpt-4o', permissionMode: 'AUTO_APPROVE', status: 'active' },
            runSnapshot: { id: 'run-root', status: 'running', verificationStatus: 'notRequested' },
            activeToolCalls: [
                { toolUseId: 'root-tool', toolName: 'Agent', input: {}, eventContext: rootContext },
                { toolUseId: 'child-tool', toolName: 'WebSearch', input: {}, eventContext: childContext },
            ],
        } as never);

        await expect(bound).resolves.toBe(true);
        const tools = useMessageStore.getState().activeToolCalls;
        expect(tools.size).toBe(1);
        expect(tools.has('sourceRun:run-root\u0000root-tool')).toBe(true);
        expect(tools.has('sourceRun:run-child\u0000child-tool')).toBe(false);
    });

    test('session switch replaces empty cost/task/coordinator projections', async () => {
        useCostStore.setState({
            sessionCost: 8,
            totalCost: 20,
            usage: { inputTokens: 9, outputTokens: 4, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
        });
        useTaskStore.getState().addTask({ taskId: 'old-task', status: 'running', createdAt: 1 });
        useCoordinatorStore.getState().addAgentTask({
            type: 'agent_spawn', taskId: 'old-agent', agentName: 'old', agentType: 'subagent',
        });

        let bindRequestId = '';
        let bindingEpoch = 0;
        const bound = bindSessionAndWait('fresh-session', payload => {
            bindRequestId = payload.bindRequestId;
            bindingEpoch = payload.bindingEpoch;
        });
        dispatch({
            ...runtimeEnvelope({ sessionId: 'fresh-session' }),
            type: 'session_restored', bindRequestId, bindingEpoch, protocolVersion: 4,
            messages: [],
            taskTree: [],
            metadata: {
                sessionId: 'fresh-session', model: 'model', permissionMode: 'DEFAULT', status: 'idle',
            },
        } as never);
        await expect(bound).resolves.toBe(true);

        expect(useCostStore.getState().sessionCost).toBe(0);
        expect(useCostStore.getState().usage.inputTokens).toBe(0);
        expect(useCostStore.getState().totalCost).toBe(0);
        expect(useTaskStore.getState().tasks.size).toBe(0);
        expect(useCoordinatorStore.getState().agentTasks).toEqual([]);
    });

    test('session_restored atomically replaces the durable Task tree projection', async () => {
        useTaskStore.getState().addTask({
            taskId: 'stale-task', status: 'running', agentName: 'stale', createdAt: 1,
        });
        let bindRequestId = '';
        let bindingEpoch = 0;
        const bound = bindSessionAndWait('task-tree-session', payload => {
            bindRequestId = payload.bindRequestId;
            bindingEpoch = payload.bindingEpoch;
        });
        dispatch({
            ...runtimeEnvelope({ sessionId: 'task-tree-session' }),
            type: 'session_restored', bindRequestId, bindingEpoch, protocolVersion: 4,
            messages: [],
            metadata: {
                sessionId: 'task-tree-session', model: 'model', permissionMode: 'DEFAULT', status: 'idle',
            },
            taskTree: [{
                id: 'root-task', sessionId: 'task-tree-session', parentTaskId: null,
                rootTaskId: 'root-task', currentRunId: 'root-run', description: 'Root research',
                taskType: 'agent', status: 'waitingDependencies', reportedProgress: 0.4,
                cleanupStatus: 'pending', verificationStatus: 'pending',
                createdAt: '2026-09-09T01:02:03.000Z',
            }, {
                id: 'child-task', sessionId: 'task-tree-session', parentTaskId: 'root-task',
                rootTaskId: 'root-task', currentRunId: 'child-run', description: 'Child research',
                taskType: 'agent', status: 'succeeded', reportedProgress: 1,
                cleanupStatus: 'confirmed', verificationStatus: 'passed',
                createdAt: '2026-09-09T01:02:04.000Z',
            }],
        } as never);
        await expect(bound).resolves.toBe(true);

        const tasks = useTaskStore.getState().tasks;
        expect(Array.from(tasks.keys())).toEqual(['root-task', 'child-task']);
        expect(tasks.has('stale-task')).toBe(false);
        expect(tasks.get('root-task')).toMatchObject({
            status: 'running', runtimeStatus: 'waitingDependencies', isCoordinator: true,
            progress: 0.4, agentName: 'Root research',
        });
        expect(tasks.get('child-task')).toMatchObject({
            status: 'completed', runtimeStatus: 'succeeded', parentTaskId: 'root-task',
        });
    });

    test('permission_request → showPermission + waiting_permission', () => {
        dispatch({
            ...runtimeEnvelope({ toolUseId: 'tu1' }),
            type: 'permission_request', ts: 1,
            toolUseId: 'tu1', toolName: 'BashTool',
            input: { command: 'rm -rf /' },
            suggestions: [],
        } as never);
        const { pendingPermissions } = usePermissionStore.getState();
        expect(pendingPermissions.length).toBe(1);
        expect(pendingPermissions[0]?.toolName).toBe('BashTool');
        expect(useSessionStore.getState().status).toBe('waiting_permission');
    });

    test('error → addMessage(system) + setStatus(idle)', () => {
        dispatch({
            ...runtimeEnvelope(),
            type: 'error', ts: 1,
            message: 'Rate limited', code: 'RATE_LIMIT', retryable: true,
        } as never);
        expect(useSessionStore.getState().status).toBe('idle');
        const msgs = useMessageStore.getState().messages;
        expect(msgs.length).toBeGreaterThan(0);
        const lastMsg = msgs[msgs.length - 1];
        expect(lastMsg.type).toBe('system');
        if (lastMsg.type === 'system') {
            expect(lastMsg.content).toContain('Rate limited');
        }
    });

    test('compact_event warning → addNotification', () => {
        const spy = vi.spyOn(useNotificationStore.getState(), 'addNotification');
        dispatch({
            ...runtimeEnvelope(),
            type: 'compact_event', ts: 1,
            phase: 'warning', usagePercent: 85,
        } as never);
        expect(spy).toHaveBeenCalledWith(
            expect.objectContaining({ key: 'compact-warning', level: 'warning' }),
        );
        spy.mockRestore();
    });

    test('root compact lifecycle updates status and adds one automatic boundary', () => {
        const root = {
            taskId: 'root-task', runId: 'root-run',
            sourceTaskId: 'root-task', sourceRunId: 'root-run',
        };
        dispatch({ ...runtimeEnvelope(root), type: 'compact_start' } as never);
        expect(useSessionStore.getState().status).toBe('compacting');

        dispatch({
            ...runtimeEnvelope(root), type: 'compact_complete',
            summary: 'auto_compact', tokensSaved: 2048,
        } as never);
        expect(useSessionStore.getState().status).toBe('streaming');
        expect(useMessageStore.getState().messages).toHaveLength(1);
        expect(useMessageStore.getState().messages[0]).toMatchObject({
            type: 'system', subtype: 'compact_boundary',
            content: '上下文已压缩，节省 2048 tokens',
        });
    });

    test('child compact lifecycle cannot mutate root status or insert a boundary', () => {
        useSessionStore.setState({ status: 'streaming' });
        const child = {
            taskId: 'root-task', runId: 'root-run',
            sourceTaskId: 'child-task', sourceRunId: 'child-run',
        };
        dispatch({ ...runtimeEnvelope(child), type: 'compact_start' } as never);
        dispatch({
            ...runtimeEnvelope(child), type: 'compact_complete',
            summary: 'auto_compact', tokensSaved: 4096,
        } as never);

        expect(useSessionStore.getState().status).toBe('streaming');
        expect(useMessageStore.getState().messages).toHaveLength(0);
    });

    test('manual root compact keeps the existing result projection', () => {
        useSessionStore.setState({ status: 'compacting' });
        dispatch({
            ...runtimeEnvelope(), type: 'compact_complete', displayText: '压缩完成',
            compactionData: { beforeTokens: 4000, afterTokens: 1500 },
        } as never);

        expect(useSessionStore.getState().status).toBe('idle');
        expect(useMessageStore.getState().messages[0]).toMatchObject({
            type: 'system', subtype: 'compact_result', content: '压缩完成',
        });
    });

    test('token_warning → addNotification', () => {
        const spy = vi.spyOn(useNotificationStore.getState(), 'addNotification');
        dispatch({
            ...runtimeEnvelope(),
            type: 'token_warning', ts: 1,
            currentTokens: 180000, maxTokens: 200000,
            usagePercent: 90, warningLevel: 'red',
        } as never);
        expect(spy).toHaveBeenCalled();
        spy.mockRestore();
    });

    test('interrupt_ack USER_INTERRUPT → idle + system message', () => {
        dispatch({
            ...runtimeEnvelope(),
            type: 'interrupt_ack', ts: 1, reason: 'USER_INTERRUPT',
        } as never);
        expect(useSessionStore.getState().status).toBe('idle');
        const msgs = useMessageStore.getState().messages;
        expect(msgs.some(m => m.type === 'system' && (m as { content: string }).content.includes('已中断'))).toBe(true);
    });

    test('model_changed → setModel', () => {
        dispatch({
            ...runtimeEnvelope(), type: 'model_changed', ts: 1, model: 'qwen3.6-plus' } as never);
        expect(useSessionStore.getState().model).toBe('qwen3.6-plus');
    });

    test('permission_mode_changed commits server mode without hiding pending requests', () => {
        usePermissionStore.getState().showPermission({
            interactionId: 'permission-1',
            toolUseId: 'tool-1',
            toolName: 'Bash',
            input: {},
            riskLevel: 'high',
            reason: 'existing request',
        });

        dispatch({
            ...runtimeEnvelope(),
            type: 'permission_mode_changed',
            mode: 'AUTO_APPROVE',
            previous: 'DEFAULT',
            ts: 1,
        } as never);

        expect(usePermissionStore.getState().permissionMode).toBe('auto_approve');
        expect(usePermissionStore.getState().pendingPermissions).toHaveLength(1);
    });

    test('permission_mode_changed ignores unknown server values', () => {
        dispatch({
            ...runtimeEnvelope(),
            type: 'permission_mode_changed',
            mode: 'UNKNOWN',
            ts: 1,
        } as never);

        expect(usePermissionStore.getState().permissionMode).toBe('default');
    });

    test('message_complete → finalizeStream + idle', async () => {
        // Start streaming first
        useMessageStore.getState().appendStreamDelta('Test response');

        dispatch({
            ...runtimeEnvelope(),
            type: 'message_complete', ts: 1,
            usage: { inputTokens: 100, outputTokens: 50, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
            stopReason: 'end_turn',
        } as never);

        // handleMessageComplete uses queueMicrotask, so we need to wait for it
        await new Promise<void>(resolve => queueMicrotask(() => resolve()));

        expect(useSessionStore.getState().status).toBe('idle');
        expect(useMessageStore.getState().streamingContent).toBe('');
    });

    test('child runtime diagnostics never create root conversation messages or tools', async () => {
        useSessionStore.setState({ sessionId: 's1', status: 'streaming' });
        const child = {
            sessionId: 's1', taskId: 'task-root', runId: 'run-root',
            sourceTaskId: 'task-child', sourceRunId: 'run-child',
        };
        dispatch({
            ...runtimeEnvelope(child),
            type: 'stream_delta', delta: 'child output', messageId: 'child-message',
        } as never);
        dispatch({
            ...runtimeEnvelope(child),
            type: 'thinking_delta', delta: 'child reasoning', messageId: 'child-message',
        } as never);
        dispatch({
            ...runtimeEnvelope({ ...child, toolUseId: 'child-search' }),
            type: 'tool_use_start', toolUseId: 'child-search',
            toolName: 'WebSearch', input: {},
        } as never);
        dispatch({
            ...runtimeEnvelope({ ...child, toolUseId: 'child-search' }),
            type: 'tool_use_input', toolUseId: 'child-search',
            toolName: 'WebSearch', input: { query: 'internal' },
        } as never);
        dispatch({
            ...runtimeEnvelope({ ...child, toolUseId: 'child-search' }),
            type: 'tool_result', toolUseId: 'child-search', content: '[]', isError: false,
        } as never);
        dispatch({
            ...runtimeEnvelope(child),
            type: 'error', code: 'CHILD_FAILED', message: 'internal failure', retryable: false,
        } as never);
        dispatch({
            ...runtimeEnvelope(child),
            type: 'message_complete',
            usage: { inputTokens: 1, outputTokens: 1, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
            stopReason: 'end_turn',
        } as never);

        await new Promise<void>(resolve => queueMicrotask(resolve));
        const state = useMessageStore.getState();
        expect(state.messages).toHaveLength(0);
        expect(state.streamingPartitions.size).toBe(0);
        expect(state.activeToolCalls.size).toBe(0);
        expect(useSessionStore.getState().status).toBe('streaming');
    });

    test('message_complete atomically reconciles the authoritative committed tail', async () => {
        useSessionStore.setState({ sessionId: 's1', status: 'streaming' });
        useMessageStore.setState({
            messages: [
                { type: 'assistant', uuid: 'anchor', timestamp: 1, content: [{ type: 'text', text: 'history' }] },
                { type: 'user', uuid: 'provisional', timestamp: 2, content: [{ type: 'text', text: 'draft' }] },
            ] as never,
        });
        useMessageStore.getState().startToolCall('tool-1', 'Bash', { command: 'pwd' });

        dispatch({
            ...runtimeEnvelope(),
            type: 'message_complete', ts: 2, sessionId: 's1', runId: 'run-1',
            replaceAfterMessageId: 'anchor',
            committedMessages: [
                { type: 'user', uuid: 'saved-user', timestamp: 3, content: [{ type: 'text', text: 'saved' }] },
                { type: 'assistant', uuid: 'saved-final', timestamp: 4, content: [{ type: 'text', text: 'done' }] },
            ],
            usage: { inputTokens: 10, outputTokens: 5, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
            stopReason: 'end_turn',
        } as never);

        await new Promise<void>(resolve => queueMicrotask(resolve));

        expect(useMessageStore.getState().messages.map(message => message.uuid))
            .toEqual(['anchor', 'saved-user', 'saved-final']);
        expect(useMessageStore.getState().activeToolCalls.size).toBe(0);
        expect(useSessionStore.getState().status).toBe('idle');
    });

    test('a late committed completion cannot replace the newly selected session', async () => {
        useSessionStore.setState({ sessionId: 's2', status: 'idle' });
        useMessageStore.setState({
            messages: [{
                type: 'assistant', uuid: 's2-message', timestamp: 1,
                content: [{ type: 'text', text: 'current session' }],
            }] as never,
        });

        dispatch({
            ...runtimeEnvelope(),
            type: 'message_complete', ts: 3, sessionId: 's1',
            replaceAfterMessageId: null,
            committedMessages: [{
                type: 'assistant', uuid: 's1-message', timestamp: 2,
                content: [{ type: 'text', text: 'stale session' }],
            }],
            usage: { inputTokens: 1, outputTokens: 1, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 },
            stopReason: 'end_turn',
        } as never);

        await new Promise<void>(resolve => queueMicrotask(resolve));

        expect(useMessageStore.getState().messages.map(message => message.uuid))
            .toEqual(['s2-message']);
        expect(useSessionStore.getState().sessionId).toBe('s2');
    });

    test('run_input_applied closes the current assistant segment without ending the run', () => {
        useSessionStore.getState().setStatus('streaming');
        useMessageStore.getState().appendStreamDelta('before steering');
        const firstAssistantId = useMessageStore.getState().streamingMessageId;

        dispatch({
            ...runtimeEnvelope(),
            type: 'run_input_applied', requestId: 'request-1',
            text: 'change direction', appliedAt: 123,
        } as never);

        let state = useMessageStore.getState();
        expect(state.streamingMessageId).toBeNull();
        expect(state.messages.map(message => message.type))
            .toEqual(['assistant', 'user']);
        expect(state.messages[0]).toMatchObject({
            uuid: firstAssistantId,
            content: [{ type: 'text', text: 'before steering' }],
        });
        expect(state.messages[1]).toMatchObject({
            uuid: 'request-1',
            content: [{ type: 'text', text: 'change direction' }],
        });
        expect(useSessionStore.getState().status).toBe('streaming');

        dispatch({
            ...runtimeEnvelope(), type: 'stream_delta', delta: 'after steering', messageId: 'next' } as never);
        state = useMessageStore.getState();
        expect(state.streamingMessageId).not.toBe(firstAssistantId);
        expect(state.messages.map(message => message.type))
            .toEqual(['assistant', 'user', 'assistant']);

        const nextAssistantId = state.streamingMessageId;
        dispatch({
            ...runtimeEnvelope(),
            type: 'run_input_applied', requestId: 'request-1',
            text: 'change direction', appliedAt: 123,
        } as never);
        expect(useMessageStore.getState().streamingMessageId)
            .toBe(nextAssistantId);
        expect(useMessageStore.getState().messages.map(message => message.type))
            .toEqual(['assistant', 'user', 'assistant']);
    });

    test('run_input_rejected only idles a stale client when no active run exists', () => {
        useSessionStore.getState().setStatus('streaming');
        dispatch({
            ...runtimeEnvelope(),
            type: 'run_input_rejected', requestId: 'request-1',
            code: 'QUEUE_FULL', message: 'full', rejectedAt: 1,
        } as never);
        expect(useSessionStore.getState().status).toBe('streaming');

        dispatch({
            ...runtimeEnvelope(),
            type: 'run_input_rejected', requestId: 'request-2',
            code: 'NO_ACTIVE_RUN', message: 'finished', rejectedAt: 2,
        } as never);
        expect(useSessionStore.getState().status).toBe('idle');
    });

    test('未知消息类型 → console.warn (不崩溃)', () => {
        const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});
        expect(() => {
            dispatch({
            ...runtimeEnvelope(), type: 'unknown_future_type', ts: 1 } as never);
        }).not.toThrow();
        expect(warnSpy).toHaveBeenCalled();
        warnSpy.mockRestore();
    });
});
