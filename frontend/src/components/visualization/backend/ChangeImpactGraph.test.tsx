import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { type ReactNode } from 'react';
import { ChangeImpactGraph } from './ChangeImpactGraph';
import { useSessionStore } from '@/store/sessionStore';
import { useProjectStore } from '@/store/projectStore';
import { useChangeImpactStore } from '@/store/changeImpactStore';

vi.mock('@xyflow/react', async () => {
  const { useState } = await import('react');
  const noop = () => {};
  return {
    ReactFlowProvider: ({ children }: { children: ReactNode }) => <>{children}</>,
    ReactFlow: ({ nodes, onNodeClick, children }: { nodes: { id: string }[]; onNodeClick: (event: unknown, node: { id: string }) => void; children: ReactNode }) => (
      <div>{nodes.map(node => <button key={node.id} onClick={() => onNodeClick({}, node)}>节点 {node.id}</button>)}{children}</div>
    ),
    useReactFlow: () => ({ fitView: noop }),
    useNodesState: (nodes: unknown[]) => [...useState(nodes), noop],
    useEdgesState: (edges: unknown[]) => [...useState(edges), noop],
    MiniMap: () => null, Controls: () => null, Background: () => null, Handle: () => null,
    Position: { Top: 'top', Bottom: 'bottom' }, BackgroundVariant: { Dots: 'dots' },
  };
});

beforeEach(() => {
  useChangeImpactStore.getState().reset();
  useSessionStore.setState({ sessionId: 's-impact' });
  useProjectStore.setState({ projects: [] });
});
afterEach(() => vi.unstubAllGlobals());

describe('change impact graph', () => {
  it('preserves summary counts and original node details', async () => {
    useChangeImpactStore.setState({ impactData: {
      changed_file: 'src/DemoService.java', changed_lines: [14, 16],
      impact_nodes: [{ id: 'api', type: 'api', name: 'DemoController.listItems', file_path: 'src/DemoController.java', line_range: [12, 28], impact_level: 'direct', confidence: 'high', language: 'Java' }],
      impact_edges: [], summary: { direct_count: 1, indirect_count: 0, potential_count: 0, affected_apis: ['/api/demo'], affected_tasks: [] },
    } });
    render(<ChangeImpactGraph />);
    expect(screen.getByText('直接影响: 1')).toBeInTheDocument();
    fireEvent.click(await screen.findByRole('button', { name: '节点 __change_source__' }));
    expect(screen.queryByText('节点详情')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '节点 api' }));
    expect(screen.getByText('src/DemoController.java')).toBeInTheDocument();
    expect(screen.getByText('L12–28')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '关闭影响节点详情' }));
    expect(useChangeImpactStore.getState().selectedNode).toBeNull();
    expect(useChangeImpactStore.getState().impactData?.impact_nodes).toHaveLength(1);
  });

  it('clears an error using the existing reset action without making a request', () => {
    useChangeImpactStore.setState({ error: '分析失败示例' });
    render(<ChangeImpactGraph />);
    expect(screen.getByText('分析失败示例')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '清除错误' }));
    expect(screen.getByText('暂无影响分析数据')).toBeInTheDocument();
    expect(useChangeImpactStore.getState().error).toBeNull();
  });
  it('submits a hinted file only after an explicit action and cancels when the panel exits', async () => {
    const fetch = vi.fn().mockImplementation((path: string) => path.endsWith('/cancel') ? Promise.resolve(new Response('{}')) : new Promise(() => {}));
    vi.stubGlobal('fetch', fetch);
    useChangeImpactStore.getState().applyVisualizationHint({ filePath: 'src/api.py', changedLines: [3, 9] });
    const view = render(<ChangeImpactGraph />);
    expect(screen.getByLabelText('影响分析文件路径')).toHaveValue('src/api.py');
    expect(screen.getByText('辅助分析，非验证结果；仍需运行实际测试。')).toBeInTheDocument();
    expect(fetch).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: '分析影响' }));
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toMatchObject({ sessionId: 's-impact', filePath: 'src/api.py', changedLines: [3, 9], depth: 3 });
    expect(screen.getByRole('button', { name: '取消分析' })).toBeInTheDocument();
    view.unmount();
    expect(fetch.mock.calls[0][1].signal.aborted).toBe(true);
    expect(fetch.mock.calls[1][0]).toBe('/api/code-analysis/cancel');
  });

  it('rejects invalid lines before requesting analysis', () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    render(<ChangeImpactGraph />);
    fireEvent.change(screen.getByLabelText('影响分析文件路径'), { target: { value: 'a.py' } });
    fireEvent.change(screen.getByLabelText('影响分析变更行号'), { target: { value: '-1,hello' } });
    fireEvent.click(screen.getByRole('button', { name: '分析影响' }));
    expect(screen.getByRole('alert')).toHaveTextContent('正整数');
    expect(fetch).not.toHaveBeenCalled();
  });

});
