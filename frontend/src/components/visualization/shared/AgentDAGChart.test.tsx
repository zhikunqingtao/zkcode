import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import type { Node, Edge } from '@xyflow/react';
import { useCoordinatorStore } from '@/store/coordinatorStore';
import { AgentDAGChart } from './AgentDAGChart';
import type { AgentTask } from '@/types';

const flow = vi.hoisted(() => ({ fitView: vi.fn(), render: vi.fn(), change: vi.fn() }));
vi.mock('@xyflow/react', async () => {
    const React = await import('react');
    return {
        ReactFlow: (props: { nodes: Node[]; edges: Edge[]; children?: React.ReactNode }) => {
            flow.render(props); return <div data-testid="graph">{props.children}</div>;
        },
        ReactFlowProvider: ({ children }: { children?: React.ReactNode }) => <>{children}</>,
        MiniMap: () => null, Background: () => null, Controls: () => null,
        useNodesState: function useNodesState(initial: Node[]) { const [nodes, setNodes] = React.useState(initial); return [nodes, setNodes, flow.change]; },
        useEdgesState: function useEdgesState(initial: Edge[]) { const [edges, setEdges] = React.useState(initial); return [edges, setEdges, flow.change]; },
        useReactFlow: () => ({ fitView: flow.fitView }), BackgroundVariant: { Dots: 'dots' },
    };
});
vi.mock('./AgentDAGNode', () => ({ AgentDAGNode: () => null }));
vi.mock('framer-motion', () => ({ useReducedMotion: () => false }));
const task = (taskId: string, startTime: number): AgentTask => ({ taskId, startTime, agentName: taskId, agentType: 'subagent', description: taskId, status: 'running' });
const graph = () => flow.render.mock.lastCall?.[0] as { nodes: Node[]; edges: Edge[] };

describe('Mounted Agent DAG', () => {
    beforeEach(() => { vi.useFakeTimers(); vi.clearAllMocks(); useCoordinatorStore.getState().clearAll(); });
    afterEach(() => { vi.useRealTimers(); });
    it('shows the empty state and reacts to real store projection then session cleanup', () => {
        render(<AgentDAGChart />);
        expect(screen.getByText('暂无 Agent 任务')).toBeInTheDocument();
        act(() => useCoordinatorStore.getState().addAgentTask({ type: 'agent_spawn', taskId: 'live', agentName: 'live', agentType: 'subagent' }));
        expect(graph().nodes[0].data.status).toBe('running');
        act(() => useCoordinatorStore.getState().failAgentTask('live', 'real failure'));
        expect(graph().nodes[0].data).toMatchObject({ status: 'failed', result: 'real failure' });
        act(() => useCoordinatorStore.getState().clearAll());
        expect(screen.getByText('暂无 Agent 任务')).toBeInTheDocument();
    });
    it('does not fabricate sequential edges between tasks within two seconds', () => {
        useCoordinatorStore.setState({ agentTasks: [task('a', 1000), task('b', 2500)] });
        render(<AgentDAGChart />);
        expect(graph().nodes.map(node => node.id)).toEqual(['a', 'b']);
        expect(graph().edges).toEqual([]);
    });
    it('marks temporal inference distinctly and preserves an explicit dependency', () => {
        useCoordinatorStore.setState({ agentTasks: [task('a', 1000), task('b', 3500)] });
        render(<AgentDAGChart />);
        expect(graph().edges[0]).toMatchObject({ source: 'a', target: 'b', style: { strokeDasharray: '5,5' } });
        act(() => useCoordinatorStore.setState({ agentTasks: [task('a', 1000), { ...task('b', 3500), parentTaskId: 'a' }] }));
        expect(graph().edges).toHaveLength(1);
        expect(graph().edges[0].style?.strokeDasharray).toBeUndefined();
    });
    it('uses actual dagre layout when the direction changes and refits the view', () => {
        useCoordinatorStore.setState({ agentTasks: [task('a', 1000), task('b', 3500)] });
        render(<AgentDAGChart />);
        const vertical = graph().nodes.map(node => node.position);
        expect(vertical[1].y).toBeGreaterThan(vertical[0].y);
        fireEvent.click(screen.getByTitle('切换为从左到右'));
        const horizontal = graph().nodes.map(node => node.position);
        expect(horizontal[1].x).toBeGreaterThan(horizontal[0].x);
        act(() => vi.advanceTimersByTime(50));
        expect(flow.fitView).toHaveBeenCalledWith({ padding: 0.2 });
    });
    it('refits new nodes but cancels a queued fit when unmounted', () => {
        const { unmount } = render(<AgentDAGChart />);
        act(() => useCoordinatorStore.setState({ agentTasks: [task('a', 1000)] }));
        act(() => vi.advanceTimersByTime(50));
        expect(flow.fitView).toHaveBeenCalledTimes(1);
        act(() => useCoordinatorStore.setState({ agentTasks: [task('a', 1000), task('b', 3500)] }));
        unmount();
        act(() => vi.advanceTimersByTime(50));
        expect(flow.fitView).toHaveBeenCalledTimes(1);
    });
});
