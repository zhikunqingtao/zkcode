import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { useExecutionPreferencesStore } from '../executionPreferencesStore';
const initial = { revision: 0, effort: 'auto', fast: false, effectiveModel: 'configured-model', fastAvailable: false, supportedEfforts: ['low', 'high'] };
const reply = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status });
beforeEach(() => useExecutionPreferencesStore.setState({ sessions: {} }));
afterEach(() => vi.unstubAllGlobals());
it('captures session identity and revision, and keeps the last confirmed value on failure', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(reply(initial)).mockResolvedValueOnce(reply({ message: 'not configured' }, 400)); vi.stubGlobal('fetch', fetch);
    await useExecutionPreferencesStore.getState().load('session-a');
    expect(await useExecutionPreferencesStore.getState().update('session-a', { fast: true })).toBe(false);
    expect(fetch.mock.calls[1][0]).toBe('/api/sessions/session-a/execution-preferences');
    expect(JSON.parse(fetch.mock.calls[1][1].body)).toEqual({ revision: 0, fast: true });
    expect(useExecutionPreferencesStore.getState().sessions['session-a'].value).toEqual(initial);
    expect(useExecutionPreferencesStore.getState().sessions['session-b']).toBeUndefined();
});
it('reloads a CAS conflict without replaying the requested mutation', async () => {
    const changed = { ...initial, revision: 2, effort: 'high' };
    const fetch = vi.fn().mockResolvedValueOnce(reply(initial)).mockResolvedValueOnce(reply({}, 409)).mockResolvedValueOnce(reply(changed)); vi.stubGlobal('fetch', fetch);
    await useExecutionPreferencesStore.getState().load('session-a');
    expect(await useExecutionPreferencesStore.getState().update('session-a', { effort: 'low' })).toBe(false);
    expect(fetch).toHaveBeenCalledTimes(3);
    expect(useExecutionPreferencesStore.getState().sessions['session-a'].value).toEqual(changed);
    expect(useExecutionPreferencesStore.getState().sessions['session-a'].error).toContain('重新加载');
});
