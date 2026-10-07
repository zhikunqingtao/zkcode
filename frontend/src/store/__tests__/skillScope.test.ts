import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { useSessionStore } from '../sessionStore';
import { skillRequest, useSkillStore, type SkillItem } from '../skillStore';
const item: SkillItem = { id: 'same-id', name: 'scope alias', description: 'A', source: 'PROJECT', enabled: true };
const response = (body: unknown) => ({ ok: true, status: 200, json: async () => body }) as Response;
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(done => { resolve = done; }); return { promise, resolve }; }
beforeEach(() => {
  useSessionStore.setState({ sessionId: null });
  useSkillStore.getState().setProjectScope(null);
  useSkillStore.setState({ skills: [], loaded: false, loading: false, error: null, stateError: null, pending: {} });
});
afterEach(() => { useSessionStore.setState({ sessionId: null }); useSkillStore.getState().setProjectScope(null); vi.unstubAllGlobals(); });
it('sends exactly one saved scope, preserving toggle query parameters', () => {
  expect(skillRequest('/api/skills')).toMatchObject({ url: '/api/skills', headers: {}, scopeKey: 'global' });
  useSkillStore.getState().setProjectScope('project a');
  expect(skillRequest('/api/skills/toggle?enabled=false')).toMatchObject({ url: '/api/skills/toggle?enabled=false&projectId=project%20a', headers: {} });
  useSessionStore.setState({ sessionId: 'session-b' });
  expect(skillRequest('/api/skills')).toEqual({ url: '/api/skills', headers: { 'X-Session-Id': 'session-b' }, scopeKey: 'session:session-b' });
});
it('clears old aliases synchronously and ignores late reads even after A → B → A', async () => {
  const old = deferred<Response>();
  const fetchMock = vi.fn().mockImplementationOnce(() => old.promise).mockResolvedValue(response({ skills: [{ ...item, description: 'fresh A' }] }));
  vi.stubGlobal('fetch', fetchMock);
  useSessionStore.setState({ sessionId: 'A' });
  const loading = useSkillStore.getState().loadSkills(); await Promise.resolve();
  const signal = fetchMock.mock.calls[0][1].signal as AbortSignal;
  useSkillStore.setState({ skills: [item], loaded: true });
  useSessionStore.setState({ sessionId: 'B' });
  expect(useSkillStore.getState().skills).toEqual([]); expect(signal.aborted).toBe(true);
  useSessionStore.setState({ sessionId: 'A' }); await useSkillStore.getState().loadSkills();
  old.resolve(response({ skills: [item] })); await loading;
  expect(useSkillStore.getState().skills[0].description).toBe('fresh A');
});
it('does not apply an old scope mutation response to the next project with the same id', async () => {
  const old = deferred<Response>(); vi.stubGlobal('fetch', vi.fn(() => old.promise));
  useSessionStore.setState({ sessionId: 'A' }); useSkillStore.setState({ skills: [item], loaded: true });
  const saving = useSkillStore.getState().toggleSkill(item.id, false);
  useSessionStore.setState({ sessionId: 'B' }); useSkillStore.setState({ skills: [{ ...item, description: 'B' }], loaded: true });
  old.resolve(response({ ...item, enabled: false })); await saving;
  expect(useSkillStore.getState().skills).toEqual([{ ...item, description: 'B' }]); expect(useSkillStore.getState().pending).toEqual({});
});
