/**
 * CoordinatorStore 单元测试 — 对应 Task3-5 方案 §11.11 资产 #9。
 *
 * Native event projection, bounded retention and session lifecycle regression.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { useCoordinatorStore } from '@/store/coordinatorStore';
import type {
    WorkflowPhaseUpdatePayload,
    AgentSpawnPayload,
    CoordinatorEventEnvelope,
} from '@/types';

function resetStore() {
    useCoordinatorStore.getState().clearAll();
}

function phaseUpdate(overrides: Partial<WorkflowPhaseUpdatePayload> = {}): WorkflowPhaseUpdatePayload {
    return {
        type: 'workflow_phase_update',
        workflowId: 'wf-1',
        phaseName: 'Research',
        status: 'RUNNING',
        phaseIndex: 0,
        totalPhases: 4,
        phasePrompt: 'initial research',
        objective: 'build a thing',
        ...overrides,
    };
}

function spawnPayload(overrides: Partial<AgentSpawnPayload> = {}): AgentSpawnPayload {
    return {
        type: 'agent_spawn',
        taskId: 't-1',
        agentName: 'researcher',
        agentType: 'research',
        ...overrides,
    };
}

describe('CoordinatorStore', () => {
    beforeEach(() => {
        resetStore();
    });

    it('CS-01 首次收到 phase_update 初始化 activeWorkflow 并展开面板', () => {
        useCoordinatorStore.getState().updateWorkflowPhase(phaseUpdate());

        const state = useCoordinatorStore.getState();
        expect(state.activeWorkflow?.workflowId).toBe('wf-1');
        expect(state.activeWorkflow?.phases).toHaveLength(4);
        expect(state.activeWorkflow?.phases[0].status).toBe('active');
        expect(state.panelVisible).toBe(true);
    });

    it('CS-02 phaseIndex 推进时之前阶段标记 completed', () => {
        const s = useCoordinatorStore.getState();
        s.updateWorkflowPhase(phaseUpdate({ phaseIndex: 0, phaseName: 'Research' }));
        s.updateWorkflowPhase(phaseUpdate({ phaseIndex: 2, phaseName: 'Implementation' }));

        const wf = useCoordinatorStore.getState().activeWorkflow!;
        expect(wf.phases[0].status).toBe('completed');
        expect(wf.phases[1].status).toBe('completed');
        expect(wf.phases[2].status).toBe('active');
    });

    it('CS-03 agent_spawn 添加任务且保持最近 50 条上限', () => {
        const s = useCoordinatorStore.getState();
        for (let i = 0; i < 60; i++) {
            s.addAgentTask(spawnPayload({ taskId: `t-${i}`, agentName: `agent-${i}` }));
        }

        const tasks = useCoordinatorStore.getState().agentTasks;
        expect(tasks).toHaveLength(50);
        expect(tasks[0].taskId).toBe('t-10');
    });

    it('CS-04 appendCoordinatorEvent 环形缓冲最多 200 条', () => {
        const s = useCoordinatorStore.getState();
        const envelope = (ts: number): CoordinatorEventEnvelope => ({
            type: 'coordinator_event',
            ts,
            uuid: `u-${ts}`,
            sessionId: 'sess-1',
            workflowId: 'wf-1',
            eventType: 'phase_transition',
            payload: {},
        });
        for (let i = 0; i < 250; i++) s.appendCoordinatorEvent(envelope(i));

        const evs = useCoordinatorStore.getState().coordinatorEvents;
        expect(evs).toHaveLength(200);
        expect(evs[0].ts).toBe(50);
    });

    it('CS-05 dismissWarning 标记警告 dismissed=true，clearDismissedWarnings 清理', () => {
        const s = useCoordinatorStore.getState();
        s.addDelegationWarning('test warning');
        const id = useCoordinatorStore.getState().delegationWarnings[0].id;

        s.dismissWarning(id);
        expect(useCoordinatorStore.getState().delegationWarnings[0].dismissed).toBe(true);

        s.clearDismissedWarnings();
        expect(useCoordinatorStore.getState().delegationWarnings).toHaveLength(0);
    });

    it('CS-13 Swarm 与 Worker 原生事件形成 DAG 唯一投影', () => {
        const store = useCoordinatorStore.getState();
        store.updateSwarmState({
            type: 'swarm_state_update',
            swarmId: 'swarm-1',
            phase: 'RUNNING',
            activeWorkers: 1,
            totalWorkers: 1,
            completedTasks: 0,
            totalTasks: 1,
            workers: {},
        });
        store.updateWorkerProgress({
            type: 'worker_progress',
            swarmId: 'swarm-1',
            workerId: 'worker-1',
            status: 'WORKING',
            currentTask: 'read files',
            toolCallCount: 0,
            tokenConsumed: 0,
            recentToolCalls: [],
            progressPercent: 10,
            totalSteps: null,
            completedSteps: null,
            errorMessage: null,
            currentStepDescription: null,
            terminationReason: null,
        });
        const swarm = useCoordinatorStore.getState().swarms.get('swarm-1');
        expect(swarm?.workers['worker-1'].progressPercent).toBe(10);
        useCoordinatorStore.getState().clearAll();
        expect(useCoordinatorStore.getState().swarms.size).toBe(0);
    });

    it('CS-14 原生 Agent 失败事件保留终态与错误', () => {
        const store = useCoordinatorStore.getState();
        store.addAgentTask(spawnPayload({ taskId: 'failed-agent', agentName: 'failed-agent' }));
        store.failAgentTask('failed-agent', 'provider stopped');

        expect(useCoordinatorStore.getState().agentTasks[0]).toMatchObject({
            taskId: 'failed-agent',
            status: 'failed',
            result: 'provider stopped',
        });
    });

    it('projects completion after progress without creating an unknown agent', () => {
        const store = useCoordinatorStore.getState();
        store.addAgentTask(spawnPayload());
        store.updateAgentTask('t-1', 'Inspecting source');
        store.completeAgentTask('unknown', 'must not appear');
        store.completeAgentTask('t-1', 'Verified result');
        expect(useCoordinatorStore.getState().agentTasks).toHaveLength(1);
        expect(useCoordinatorStore.getState().agentTasks[0]).toMatchObject({
            taskId: 't-1', progress: 'Inspecting source', status: 'completed', result: 'Verified result',
        });
    });

    it('ends the active phase and clears private projections before another session', () => {
        const store = useCoordinatorStore.getState();
        store.updateWorkflowPhase(phaseUpdate());
        store.addAgentTask(spawnPayload());
        store.addDelegationWarning('Private warning');
        store.addMailboxEvent({ from: 'a', to: 'b', contentType: 'task_spec', messageSize: 50, timestamp: 1 });
        store.appendCoordinatorEvent({ type: 'coordinator_event', ts: 1, uuid: 'event', sessionId: 'old-session', workflowId: 'wf-1', eventType: 'phase_transition', payload: {} });
        store.updateWorkflowPhase(phaseUpdate({ phaseIndex: -1, status: 'COMPLETED' }));
        expect(useCoordinatorStore.getState().activeWorkflow?.phases[0]).toMatchObject({ status: 'completed', endTime: expect.any(Number) });
        store.clearAll();
        const cleared = useCoordinatorStore.getState();
        expect(cleared.activeWorkflow).toBeNull();
        expect(cleared.agentTasks).toEqual([]);
        expect(cleared.delegationWarnings).toEqual([]);
        expect(cleared.coordinatorEvents).toEqual([]);
        expect(cleared.mailboxEvents).toEqual([]);
        expect(cleared.swarms.size).toBe(0);
        store.updateWorkflowPhase(phaseUpdate({ workflowId: 'new-session-workflow' }));
        expect(useCoordinatorStore.getState().activeWorkflow?.workflowId).toBe('new-session-workflow');
        expect(useCoordinatorStore.getState().agentTasks).toEqual([]);
    });

    it('bounds warning retention without retaining dismissed warnings', () => {
        const store = useCoordinatorStore.getState();
        for (let index = 0; index < 25; index++) store.addDelegationWarning(`warning-${index}`);
        const warnings = useCoordinatorStore.getState().delegationWarnings;
        expect(warnings).toHaveLength(20);
        expect(warnings[0].message).toBe('warning-5');
        expect(warnings[19].message).toBe('warning-24');
        store.dismissWarning(warnings[0].id);
        store.clearDismissedWarnings();
        expect(useCoordinatorStore.getState().delegationWarnings).toHaveLength(19);
        expect(useCoordinatorStore.getState().delegationWarnings[0].message).toBe('warning-6');
    });
    // Legacy clearCoordinatorEvents/panelVisible setters have no production consumer.
    // Their empty reservations were removed; actual sidebar visibility and lifecycle
    // behavior are verified by Sidebar/AppLayout and the mounted DAG tests.
});
