import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { APISequenceDiagram } from './APISequenceDiagram';
import { useMessageStore } from '@/store/messageStore';
import { useSessionStore } from '@/store/sessionStore';
import { useSequenceViewStore } from '@/store/sequenceViewStore';

vi.mock('@/components/visualization/shared/MermaidBlock', () => ({
  default: ({ code }: { code: string }) => <pre data-testid="diagram-source">{code}</pre>,
}));

beforeEach(() => {
  useSessionStore.setState({ sessionId: 'sequence-session' });
  useSequenceViewStore.getState().reset();
  useMessageStore.setState({ messages: [
    { type: 'assistant', uuid: 'call', timestamp: 1000, stopReason: 'tool_use', usage: {inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0}, content: [
      { type: 'tool_use', toolUseId: 'read', toolName: 'Read', input: { path: 'demo.ts' } },
      { type: 'tool_use', toolUseId: 'bash', toolName: 'Bash', input: { command: 'npm test' } },
    ] },
    { type: 'user', uuid: 'result', timestamp: 2000, content: [
      { type: 'tool_result', toolUseId: 'read', content: '示例文件内容', isError: false },
      { type: 'tool_result', toolUseId: 'bash', content: '示例命令失败', isError: true },
    ] },
  ] });
});

describe('API sequence panel', () => {
  it('filters the chart and records together, closes with Escape, and restores all calls', () => {
    render(<APISequenceDiagram />);
    fireEvent.click(screen.getByRole('button', { name: '过滤工具' }));
    fireEvent.click(screen.getByRole('checkbox', { name: 'Read' }));
    expect(screen.queryByRole('button', { name: /Bash\s*command/ })).not.toBeInTheDocument();
    expect(screen.getByTestId('diagram-source')).not.toHaveTextContent('Bash');
    fireEvent.keyDown(screen.getByRole('checkbox', { name: 'Read' }), { key: 'Escape' });
    expect(screen.queryByRole('checkbox')).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /过滤工具/ }));
    fireEvent.click(screen.getByRole('button', { name: '清除全部' }));
    expect(screen.getByRole('button', { name: /Bash\s*command/ })).toBeInTheDocument();
  });

  it('retains raw inputs and failures in details and preserves them on refresh', () => {
    render(<APISequenceDiagram />);
    fireEvent.click(screen.getByRole('button', { name: /Bash\s*command/ }));
    expect(screen.getByText(/"command": "npm test"/)).toBeInTheDocument();
    expect(screen.getByText('示例命令失败')).toBeInTheDocument();
    expect(screen.getByText('（失败）')).toBeInTheDocument();
    fireEvent.click(screen.getByTitle('刷新'));
    expect(screen.getByText('示例命令失败')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '关闭调用详情' }));
    expect(screen.queryByText('Bash 详情')).not.toBeInTheDocument();
  });
});

it('uses only existing records for hints and never accepts fabricated calls or results', () => {
  useSequenceViewStore.getState().applyVisualizationHint({ toolUseId: 'bash', content: 'invented success', isError: false });
  render(<APISequenceDiagram />);
  expect(screen.getByText('示例命令失败')).toBeInTheDocument();
  expect(screen.queryByRole('button', { name: /Read\s*path/ })).not.toBeInTheDocument();
  expect(screen.queryByText('invented success')).not.toBeInTheDocument();
  act(() => useSequenceViewStore.getState().applyVisualizationHint({ toolUseId: 'invented', tools: [{ toolName: 'Danger', result: 'fake' }] }));
  expect(screen.getByRole('status')).toHaveTextContent('没有匹配');
  expect(screen.queryByText('Bash 详情')).not.toBeInTheDocument();
  expect(screen.queryByText('Danger')).not.toBeInTheDocument();
});

it('clears selection and filters across session switches even when the tool id is reused', () => {
  render(<APISequenceDiagram />);
  fireEvent.click(screen.getByRole('button', { name: /Bash\s*command/ }));
  expect(screen.getByText('示例命令失败')).toBeInTheDocument();
  act(() => {
    useSessionStore.setState({ sessionId: 'another-session' });
    useMessageStore.setState({ messages: [{ type: 'assistant', uuid: 'new', timestamp: 3000, stopReason: 'tool_use', usage: {inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0}, content: [{ type: 'tool_use', toolUseId: 'bash', toolName: 'Read', input: { path: 'new.txt' } }] }] });
  });
  expect(screen.queryByText('示例命令失败')).not.toBeInTheDocument();
  expect(screen.queryByText('Bash 详情')).not.toBeInTheDocument();
  expect(screen.getByRole('button', { name: /Read.*结果待确认/ })).toBeInTheDocument();
  expect(useSequenceViewStore.getState().tools).toEqual([]);
});

it('renders the actual restored inline failure and does not call pending tools successful', () => {
  const history = useMessageStore.getState().messages;
  useMessageStore.getState().restoreSessionSnapshot(history, []);
  render(<APISequenceDiagram />);
  fireEvent.click(screen.getByRole('button', { name: /Bash\s*command/ }));
  expect(screen.getByText('示例命令失败')).toBeInTheDocument();
  expect(screen.getByText('（失败）')).toBeInTheDocument();
  act(() => useMessageStore.setState({ messages: [{ type: 'assistant', uuid: 'pending', timestamp: 1000, stopReason: 'tool_use', usage: {inputTokens: 0, outputTokens: 0, cacheReadInputTokens: 0, cacheCreationInputTokens: 0}, content: [{ type: 'tool_use', toolUseId: 'pending', toolName: 'Read', input: { path: 'pending.txt' } }] }] }));
  expect(screen.queryByText('示例命令失败')).not.toBeInTheDocument();
  expect(screen.getByTestId('diagram-source')).toHaveTextContent('结果待确认');
  expect(screen.getByTestId('diagram-source')).not.toHaveTextContent('成功');
});
