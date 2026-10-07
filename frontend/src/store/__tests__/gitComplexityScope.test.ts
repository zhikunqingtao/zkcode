import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useCodeInsightStore } from '../codeInsightStore';
import { useComplexityStore } from '../complexityStore';
import { useSessionStore } from '../sessionStore';
import { useProjectStore } from '../projectStore';

const response = (data: unknown) => new Response(JSON.stringify(data), { headers: { 'Content-Type': 'application/json' } });
const metrics = { success: true, data: { root: { name: 'project', type: 'project', loc: 12, cc: 2, mi: 80, risk_level: 'A' }, stats: { total_files: 1, avg_cc: 2, high_risk_count: 0, analysis_time_ms: 1 }, cached: false, truncated: false, analysis_kind: 'heuristic', is_verification_evidence: false } };
const commit = { sha: 'a'.repeat(40), message: 'initial', author: 'fixture', date: '2026-10-07T00:00:00Z', files: ['空 格.py'] };
beforeEach(() => {
  useComplexityStore.getState().reset(); useCodeInsightStore.getState().clearAll();
  useSessionStore.setState({ sessionId: 'session-a' }); useProjectStore.setState({ projects: [] });
});
afterEach(() => vi.unstubAllGlobals());

describe('authorized complexity and Git panels', () => {
  it('uses the captured Session and real heuristic-only complexity response', async () => {
    const fetch = vi.fn().mockResolvedValue(response(metrics)); vi.stubGlobal('fetch', fetch);
    await useComplexityStore.getState().fetchComplexity('', 'src', ['python']);
    expect(fetch.mock.calls[0][0]).toBe('/api/code-quality/complexity');
    const init = fetch.mock.calls[0][1];
    expect(init.headers['X-Session-Id']).toBe('session-a');
    expect(JSON.parse(init.body)).toMatchObject({ sessionId: 'session-a', targetPath: 'src', languages: ['python'] });
    expect(useComplexityStore.getState().complexityTree).toEqual(metrics.data.root);
    fetch.mockResolvedValue(response({ ...metrics, data: { ...metrics.data, is_verification_evidence: true } }));
    await useComplexityStore.getState().fetchComplexity('');
    expect(useComplexityStore.getState().complexityTree).toBeNull();
    expect(useComplexityStore.getState().error).toContain('无效');
  });
  it('cancels late metrics and clears project data on Session changes', async () => {
    let resolve!: (value: Response) => void;
    const fetch = vi.fn().mockImplementation((path: string) => path.endsWith('/cancel') ? Promise.resolve(response({})) : new Promise<Response>(done => { resolve = done; })); vi.stubGlobal('fetch', fetch);
    const pending = useComplexityStore.getState().fetchComplexity(''); const init = fetch.mock.calls[0][1];
    useSessionStore.setState({ sessionId: 'session-b' });
    expect(init.signal.aborted).toBe(true);
    expect(JSON.parse(fetch.mock.calls[1][1].body).sessionId).toBe('session-a');
    resolve(response(metrics)); await pending;
    expect(useComplexityStore.getState()).toMatchObject({ complexityTree: null, lastRequest: null, isLoading: false });
  });
  it('pages an immutable Git head and requests root-commit diff by exact commit', async () => {
    const second = { ...commit, sha: 'b'.repeat(40) };
    const fetch = vi.fn().mockResolvedValueOnce(response({ success: true, data: { commits: [commit], total: 2, head: commit.sha } })).mockResolvedValueOnce(response({ success: true, data: { commits: [second], total: 2, head: commit.sha } })).mockResolvedValueOnce(response({ success: true, data: { summary: '1 file', detailed: '+first', files_changed: 1 } })); vi.stubGlobal('fetch', fetch);
    await useCodeInsightStore.getState().fetchGitLog(''); await useCodeInsightStore.getState().fetchMoreGitLog('');
    expect(useCodeInsightStore.getState().gitCommits).toEqual([commit, second]);
    expect(JSON.parse(fetch.mock.calls[1][1].body)).toMatchObject({ branch: commit.sha, offset: 1, sessionId: 'session-a' });
    await useCodeInsightStore.getState().fetchGitDiff('', `${commit.sha}~1`, commit.sha);
    expect(JSON.parse(fetch.mock.calls[2][1].body)).toMatchObject({ commit: commit.sha });
    expect(JSON.parse(fetch.mock.calls[2][1].body).ref1).toBeUndefined();
  });
  it('rejects stale Git responses and sends cancellation to the native owner', async () => {
    let resolve!: (value: Response) => void;
    const fetch = vi.fn().mockImplementation((path: string) => path.endsWith('/cancel') ? Promise.resolve(response({})) : new Promise<Response>(done => { resolve = done; })); vi.stubGlobal('fetch', fetch);
    const pending = useCodeInsightStore.getState().fetchGitLog(''); const init = fetch.mock.calls[0][1];
    useSessionStore.setState({ sessionId: 'session-b' });
    expect(init.signal.aborted).toBe(true); expect(fetch.mock.calls[1][0]).toBe('/api/git/cancel');
    expect(JSON.parse(fetch.mock.calls[1][1].body)).toMatchObject({ sessionId: 'session-a', requestId: JSON.parse(init.body).requestId });
    resolve(response({ success: true, data: { commits: [commit], total: 1, head: commit.sha } })); await pending;
    expect(useCodeInsightStore.getState()).toMatchObject({ gitCommits: [], gitHead: null, gitLoading: false });
  });
  it('keeps diff and blame failures visible and refuses file identity substitution', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(new Response('failure', { status: 500 })).mockResolvedValueOnce(response({ success: true, data: { file_path: 'other.py', lines: [], total_lines: 0 } })); vi.stubGlobal('fetch', fetch);
    await useCodeInsightStore.getState().fetchGitDiff('', 'old', 'new');
    expect(useCodeInsightStore.getState().diffError).toBeTruthy();
    await useCodeInsightStore.getState().fetchGitBlame('', 'private.py', commit.sha);
    expect(useCodeInsightStore.getState().activeBlame).toBeNull(); expect(useCodeInsightStore.getState().blameError).toContain('无效');
  });
});
