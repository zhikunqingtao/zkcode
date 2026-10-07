import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import VisualizationMessage from './VisualizationMessage';
import { useAppUiStore } from '@/store/appUiStore';
import { useChangeImpactStore } from '@/store/changeImpactStore';
import { useCodePathStore } from '@/store/codePathStore';
import { useSequenceViewStore } from '@/store/sequenceViewStore';
import { useApiContractStore } from '@/store/apiContractStore';
import type { Message } from '@/types';
const mounted = vi.hoisted(() => vi.fn());
vi.mock('@/components/visualization/shared/GitTimeline', () => ({ GitTimeline: () => { mounted('git'); return <p>Actual Git view</p>; } }));
vi.mock('@/components/visualization/shared/MermaidBlock', () => ({ default: () => { mounted('mermaid'); return <p>Actual Mermaid</p>; } }));
vi.mock('@/components/visualization/backend/SchemaViewer', () => ({ default: () => { mounted('schema'); return <p>Actual Schema</p>; } }));
function message(viewType: string, props: Record<string, unknown>): Extract<Message, { type: 'visualization' }> {
    return { type: 'visualization', uuid: 'visualization', timestamp: Date.now(), viewType, props };
}
beforeEach(() => { mounted.mockClear(); vi.stubGlobal('fetch', vi.fn()); useAppUiStore.setState({ pendingVisualizationTab: null, mobileNavTab: null }); useChangeImpactStore.setState({ lastHint: null }); });
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
it.each(['git-timeline', 'mermaid', 'schema-viewer'])('does not mount or fetch actual %s for classification suggestions even with fabricated props', async viewType => {
    render(<VisualizationMessage message={message(viewType, { intentOnly: true, source: 'graph TD; A-->B', schema: {}, reason: '<img src=x>' })} />);
    expect(screen.getByText('可视化建议，尚未执行分析。')).toBeInTheDocument();
    expect(screen.getByText('<img src=x>')).toBeInTheDocument();
    expect(document.querySelector('img')).toBeNull(); expect(mounted).not.toHaveBeenCalled(); expect(fetch).not.toHaveBeenCalled();
    expect(useAppUiStore.getState().pendingVisualizationTab).toBeNull();
});
it('seeds advisory hints only after explicit navigation, without executing analysis', () => {
    render(<VisualizationMessage message={message('change-impact-graph', { intentOnly: true, filePath: 'api.py', changedLines: [10] })} />);
    expect(useChangeImpactStore.getState().lastHint).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: '打开对应面板' }));
    expect(useChangeImpactStore.getState().lastHint).toMatchObject({ filePath: 'api.py', intentOnly: true });
    expect(useAppUiStore.getState().pendingVisualizationTab).toBe('impact'); expect(fetch).not.toHaveBeenCalled();
});
it('keeps ordinary actual-data rendering intact', async () => {
    render(<VisualizationMessage message={message('mermaid', { source: 'graph TD; A-->B' })} />);
    await waitFor(() => expect(mounted).toHaveBeenCalledWith('mermaid'));
    expect(screen.queryByText('可视化建议，尚未执行分析。')).not.toBeInTheDocument();
});

it('opens real code-path cards on compact layouts and prefills only after navigation', () => {
    useCodePathStore.getState().reset();
    render(<VisualizationMessage message={message('code-path-tracer', { entryFile: 'lib.py', entryFunction: 'helper' })} />);
    expect(useCodePathStore.getState().entryFile).toBe('');
    fireEvent.click(screen.getByRole('button', { name: '在可视化面板查看' }));
    expect(useCodePathStore.getState()).toMatchObject({ entryFile: 'lib.py', entryFunction: 'helper' });
    expect(useAppUiStore.getState().mobileNavTab).toBe('code-path');
    expect(fetch).not.toHaveBeenCalled();
});
it('routes sequence hints to actual-record preferences without changing API contracts', () => {
    useApiContractStore.setState({ lastHint: null });
    useSequenceViewStore.getState().reset();
    render(<VisualizationMessage message={message('api-sequence-diagram', { toolUseId: 'missing', content: 'fabricated' })} />);
    fireEvent.click(screen.getByRole('button', { name: '在可视化面板查看' }));
    expect(useApiContractStore.getState().lastHint).toBeNull();
    expect(useSequenceViewStore.getState().note).toContain('没有匹配');
    expect(useAppUiStore.getState().mobileNavTab).toBe('sequence');
    expect(fetch).not.toHaveBeenCalled();
});
