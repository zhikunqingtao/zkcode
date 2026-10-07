import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest';
import { useSessionStore } from '@/store/sessionStore';
import { useProjectStore } from '@/store/projectStore';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { type ReactNode } from 'react';
import { CodePathTracer } from './CodePathTracer';
import { useCodePathStore } from '@/store/codePathStore';

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
const endpoint = { httpMethod: 'GET', path: '/api/demo', handlerFunction: 'listItems', handlerClass: 'DemoController', filePath: 'src/DemoController.java', lineNumber: 12, language: 'Java', parameters: [] };
const node = { id: 'controller', name: 'listItems', className: 'DemoController', filePath: endpoint.filePath, lineRange: [12, 28], layer: 'controller' as const, nodeType: 'method', annotations: [], parameters: [], returnType: 'List<Item>' };

beforeEach(() => {
  useSessionStore.setState({ sessionId: "analysis-session" });
  useProjectStore.setState({ projects: [{ id: "demo-project", name: "Demo", workspaceRoot: "/tmp/demo", createdAt: "2026-10-07" }] });
  useCodePathStore.getState().reset();
  useCodePathStore.setState({ endpoints: [], projectRoot: '', endpointsLoading: false });
});
afterEach(() => vi.unstubAllGlobals());

describe('code path panel', () => {
  it('preserves scan and trace payloads through search and endpoint selection', async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({ success: true, total: 1, endpoints: [endpoint] }) }).mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({ nodes: [node], edges: [], layers: [] }) });
    vi.stubGlobal('fetch', fetchMock);
    render(<CodePathTracer />);
    fireEvent.change(screen.getByRole('textbox', { name: '项目路径' }), { target: { value: '/tmp/demo' } });
    fireEvent.click(screen.getByRole('button', { name: '扫描' }));
    await screen.findByRole('button', { name: /\/api\/demo/ });
    fireEvent.change(screen.getByRole('textbox', { name: '搜索端点' }), { target: { value: 'absent' } });
    expect(screen.getByText('无匹配端点')).toBeInTheDocument();
    fireEvent.change(screen.getByRole('textbox', { name: '搜索端点' }), { target: { value: 'demo' } });
    fireEvent.click(screen.getByRole('button', { name: /\/api\/demo/ }));
    await screen.findByRole('button', { name: '节点 controller' });
    expect(fetchMock.mock.calls[0][0]).toBe('/api/code-path/endpoints');
    expect(fetchMock.mock.calls[1][0]).toBe('/api/code-path/trace');
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ projectId: 'demo-project', requestId: expect.any(String), projectRoot: '/tmp/demo' });
    expect(JSON.parse(fetchMock.mock.calls[1][1].body)).toEqual({ projectId: 'demo-project', requestId: expect.any(String), projectRoot: '/tmp/demo', entryFile: endpoint.filePath, entryFunction: endpoint.handlerFunction, maxDepth: 10 });
    fireEvent.click(screen.getByRole('button', { name: '节点 controller' }));
    expect(screen.getByText(endpoint.filePath)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '关闭节点详情' }));
    expect(screen.queryByText('节点详情')).not.toBeInTheDocument();
  });

  it('keeps a scan failure visible and re-enables retry', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('扫描失败示例')));
    render(<CodePathTracer />);
    fireEvent.click(screen.getByRole('button', { name: '扫描' }));
    await screen.findByText('扫描失败示例');
    await waitFor(() => expect(screen.getByRole('button', { name: '扫描' })).toBeEnabled());
  });
});

it('prefills a non-API entry from a hint but sends only an explicit authorized trace', async () => {
  const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ nodes: [node], edges: [], layers: [] }) });
  vi.stubGlobal('fetch', fetchMock);
  useCodePathStore.getState().applyVisualizationHint({ entryFile: 'src/core.py', entryFunction: 'compute', maxDepth: 4, nodes: [{ fake: true }] });
  render(<CodePathTracer />);
  expect(screen.getByRole('textbox', { name: '入口文件' })).toHaveValue('src/core.py');
  expect(screen.getByRole('textbox', { name: '入口函数' })).toHaveValue('compute');
  expect(screen.getByRole('spinbutton', { name: '追踪深度' })).toHaveValue(4);
  expect(fetchMock).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole('button', { name: '追踪函数' }));
  await screen.findByRole('button', { name: '节点 controller' });
  expect(fetchMock.mock.calls[0][0]).toBe('/api/code-path/trace');
  const init = fetchMock.mock.calls[0][1];
  expect(init.headers['X-Session-Id']).toBe('analysis-session');
  expect(JSON.parse(init.body)).toEqual({ sessionId: 'analysis-session', requestId: expect.any(String), entryFile: 'src/core.py', entryFunction: 'compute', maxDepth: 4 });
  act(() => useSessionStore.setState({ sessionId: 'next-session' }));
  expect(screen.getByRole('textbox', { name: '入口文件' })).toHaveValue('');
  expect(screen.queryByRole('button', { name: '节点 controller' })).not.toBeInTheDocument();
});

it('supports manual file/function input without scanning endpoints and refuses invalid depth', async () => {
  vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ nodes: [], edges: [], layers: [] }) }));
  render(<CodePathTracer />);
  expect(screen.getByRole('button', { name: '追踪函数' })).toBeDisabled();
  fireEvent.change(screen.getByRole('textbox', { name: '入口文件' }), { target: { value: 'core.rs' } });
  fireEvent.change(screen.getByRole('textbox', { name: '入口函数' }), { target: { value: 'main' } });
  await act(() => useCodePathStore.getState().traceCodePath('core.rs', 'main', 21));
  expect(fetch).not.toHaveBeenCalled();
  expect(screen.getByText('请输入文件路径和函数名，深度须为 1–20 的整数')).toBeInTheDocument();
  fireEvent.click(screen.getByRole('button', { name: '追踪函数' }));
  await screen.findByText('未发现调用路径');
  expect(fetch).toHaveBeenCalledTimes(1);
});
