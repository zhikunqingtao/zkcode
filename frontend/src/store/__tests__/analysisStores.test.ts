import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest';
import { useDiagramStore } from '../diagramStore';
import { useCodePathStore } from '../codePathStore';
import { useSessionStore } from '../sessionStore';
import { useChangeImpactStore, cancelPendingChangeImpactAnalysis } from '../changeImpactStore';
import { useProjectStore } from '../projectStore';

const diagram = {
  diagramType: 'sequence', mermaidSyntax: 'sequenceDiagram\nA->>B: fetch', confidenceScore: 0.9,
  metadata: { nodesCount: 2, edgesCount: 1, languagesAnalyzed: ['python'], analysisTimeMs: 3 }, warnings: [],
};
const response = (value: unknown) => new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } });

describe('authorized analysis stores', () => {
  beforeEach(() => {
    useDiagramStore.getState().clearDiagram();
    useCodePathStore.getState().reset();
    useChangeImpactStore.getState().reset();
    useSessionStore.setState({ sessionId: 'session-a' });
    useProjectStore.setState({ projects: [] });
    useDiagramStore.setState({ projectRoot: '.', target: 'fetch', depth: 3, diagramType: 'sequence' });
    useCodePathStore.setState({ projectRoot: '' });
  });
  afterEach(() => vi.unstubAllGlobals());

  it('uses the source public contract with captured session authority', async () => {
    const fetch = vi.fn().mockResolvedValue(response(diagram));
    vi.stubGlobal('fetch', fetch);
    await useDiagramStore.getState().generateDiagram();
    const [url, init] = fetch.mock.calls[0];
    expect(url).toBe('/api/code-diagrams/generate');
    expect(init.headers['X-Session-Id']).toBe('session-a');
    expect(JSON.parse(init.body)).toMatchObject({ sessionId: 'session-a', projectRoot: '.', target: 'fetch', diagramType: 'sequence', depth: 3 });
    expect(JSON.parse(init.body).requestId).toMatch(/^[\da-f-]{36}$/);
    expect(useDiagramStore.getState().result).toEqual(diagram);
  });

  it('does not let a path authorize an unbound analysis', async () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch);
    useSessionStore.setState({ sessionId: null });
    useDiagramStore.setState({ target: 'fetch', projectRoot: '/unregistered' });
    await useDiagramStore.getState().generateDiagram();
    expect(fetch).not.toHaveBeenCalled();
    expect(useDiagramStore.getState().error).toContain('已授权');
    expect(useDiagramStore.getState().loading).toBe(false);
  });

  it('rejects malformed Python-shaped results instead of displaying an empty success', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({ mermaid_syntax: 'flowchart TD' })));
    await useDiagramStore.getState().generateDiagram();
    expect(useDiagramStore.getState().result).toBeNull();
    expect(useDiagramStore.getState().error).toContain('无效');
  });

  it('cancels the old owner and ignores a response after switching sessions', async () => {
    let resolve!: (value: Response) => void;
    const fetch = vi.fn().mockImplementation((path: string) => path.includes('/cancel')
      ? Promise.resolve(response({ cancelled: true }))
      : new Promise<Response>(done => { resolve = done; }));
    vi.stubGlobal('fetch', fetch);
    const pending = useDiagramStore.getState().generateDiagram();
    const first = fetch.mock.calls[0][1];
    useSessionStore.setState({ sessionId: 'session-b' });
    expect(first.signal.aborted).toBe(true);
    const cancel = fetch.mock.calls.find(([path]) => path === '/api/code-analysis/cancel');
    expect(JSON.parse(cancel![1].body)).toMatchObject({ sessionId: 'session-a', requestId: JSON.parse(first.body).requestId });
    resolve(response(diagram)); await pending;
    expect(useDiagramStore.getState().result).toBeNull();
    expect(useDiagramStore.getState().error).toBeNull();
    expect(useDiagramStore.getState().loading).toBe(false);
  });

  it('uses endpoint and trace DTOs without silently unwrapping malformed results', async () => {
    const endpoint = { httpMethod: 'GET', path: '/users', handlerFunction: 'get_user', handlerClass: '', filePath: 'api.py', lineNumber: 3, language: 'python', parameters: [] };
    const graph = { nodes: [{ id: 'get_user', name: 'get_user', className: '', filePath: 'api.py', lineRange: [3, 5], layer: 'controller', nodeType: 'api', annotations: [], parameters: [], returnType: 'dict' }], edges: [], layers: [{ layer: 'controller', nodeCount: 1, description: 'entry' }] };
    const fetch = vi.fn().mockResolvedValueOnce(response({ endpoints: [endpoint], total: 1, success: true })).mockResolvedValueOnce(response(graph)).mockResolvedValueOnce(response({ success: true, data: graph }));
    vi.stubGlobal('fetch', fetch);
    await useCodePathStore.getState().fetchEndpoints();
    expect(useCodePathStore.getState().endpoints).toEqual([endpoint]);
    await useCodePathStore.getState().traceCodePath('api.py', 'get_user', 5);
    expect(useCodePathStore.getState().pathResult).toEqual(graph);
    expect(JSON.parse(fetch.mock.calls[1][1].body)).toMatchObject({ entryFile: 'api.py', entryFunction: 'get_user', maxDepth: 5, sessionId: 'session-a' });
    await useCodePathStore.getState().traceCodePath('api.py', 'get_user');
    expect(useCodePathStore.getState().pathResult).toBeNull();
    expect(useCodePathStore.getState().error).toContain('无效');
  });

  it('aborts a pending registered-project request when its grant is revoked', async () => {
    useProjectStore.setState({ projects: [{ id: 'project-a', name: 'A', workspaceRoot: '/registered', createdAt: '2026-10-07' }] });
    useDiagramStore.setState({ projectRoot: '/registered' });
    let complete!: (response: Response) => void;
    const fetch = vi.fn().mockImplementation((path: string) => path.endsWith('/cancel') ? Promise.resolve(response({ cancellationRequested: true })) : new Promise<Response>(resolve => { complete = resolve; }));
    vi.stubGlobal('fetch', fetch);
    const pending = useDiagramStore.getState().generateDiagram();
    useProjectStore.setState({ projects: [] });
    expect(fetch.mock.calls[0][1].signal.aborted).toBe(true);
    expect(JSON.parse(fetch.mock.calls[1][1].body)).toMatchObject({ projectId: 'project-a' });
    complete(response(diagram)); await pending;
    expect(useDiagramStore.getState().result).toBeNull();
    expect(useDiagramStore.getState().loading).toBe(false);
  });
  it('binds impact requests to the project and refuses unverifiable result kinds', async () => {
    useProjectStore.setState({ projects: [{ id: 'project-a', name: 'A', workspaceRoot: '/registered', createdAt: '2026-10-07' }] });
    const impact = { changed_file: 'a.py', changed_lines: [2], impact_nodes: [], impact_edges: [], summary: { direct_count: 0, indirect_count: 0, potential_count: 0, affected_apis: [], affected_tasks: [] }, analysis_kind: 'advisory', is_verification_evidence: false, truncated: true };
    const fetch = vi.fn().mockResolvedValueOnce(response({ success: true, data: impact, elapsed_ms: 5 })).mockResolvedValueOnce(response({ success: true, data: { ...impact, analysis_kind: 'verified' } }));
    vi.stubGlobal('fetch', fetch);
    await useChangeImpactStore.getState().fetchChangeImpact('a.py', [2], '/registered', 2);
    const [url, init] = fetch.mock.calls[0];
    expect(url).toBe('/api/analysis/change-impact');
    expect(JSON.parse(init.body)).toMatchObject({ projectId: 'project-a', projectRoot: '/registered', filePath: 'a.py', changedLines: [2], depth: 2 });
    expect(JSON.parse(init.body).requestId).toMatch(/^[\da-f-]{36}$/);
    expect(useChangeImpactStore.getState().impactData).toEqual(impact);
    await useChangeImpactStore.getState().fetchChangeImpact('a.py', [2], '/registered');
    expect(useChangeImpactStore.getState().impactData).toBeNull();
    expect(useChangeImpactStore.getState().error).toContain('无效');
  });

  it('cancels and ignores superseded, exited, and previous-session impact responses', async () => {
    const completions: ((value: Response) => void)[] = [];
    const fetch = vi.fn().mockImplementation((path: string) => path.endsWith('/cancel') ? Promise.resolve(response({ cancelled: true })) : new Promise<Response>(resolve => completions.push(resolve)));
    vi.stubGlobal('fetch', fetch);
    const first = useChangeImpactStore.getState().fetchChangeImpact('a.py', [2], '');
    const firstInit = fetch.mock.calls[0][1];
    const second = useChangeImpactStore.getState().fetchChangeImpact('b.py', [3], '');
    expect(firstInit.signal.aborted).toBe(true);
    expect(JSON.parse(fetch.mock.calls[1][1].body)).toMatchObject({ sessionId: 'session-a', requestId: JSON.parse(firstInit.body).requestId });
    const secondInit = fetch.mock.calls[2][1];
    useSessionStore.setState({ sessionId: 'session-b' });
    expect(secondInit.signal.aborted).toBe(true);
    completions[1](response({ success: true, data: {} }));
    completions[0](response({ success: true, data: {} }));
    await Promise.all([first, second]);
    expect(useChangeImpactStore.getState()).toMatchObject({ impactData: null, isLoading: false, error: null });
    const third = useChangeImpactStore.getState().fetchChangeImpact('c.py', [], '');
    cancelPendingChangeImpactAnalysis();
    completions[2](response({ success: true, data: {} }));
    await third;
    expect(useChangeImpactStore.getState()).toMatchObject({ impactData: null, isLoading: false, error: null });
  });

});
