import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useMemoryStore } from './memoryStore';
const doc = (content: string, revision = 1) => ({ content, entries: [], revision, updatedAt: null, maxSize: 100000 });
const response = (content: unknown, status = 200) => ({ ok: status < 400, status, json: async () => content });
beforeEach(() => useMemoryStore.setState({ scope: 'global', projectPath: '', content: '', entries: [], revision: 0, loaded: false, loading: false, saving: false, conflict: false, dirty: false, error: null }));
afterEach(() => vi.unstubAllGlobals());
describe('SQLite memory document contract', () => {
    it('saves with the revision and scope and adopts only the server response', async () => {
        const fetchMock = vi.fn().mockResolvedValueOnce(response(doc('initial', 3))).mockResolvedValueOnce(response(doc('normalized', 4)));
        vi.stubGlobal('fetch', fetchMock);
        await useMemoryStore.getState().setScope('project', '/tmp/project');
        expect(await useMemoryStore.getState().saveRaw('draft')).toBe(true);
        expect(JSON.parse(fetchMock.mock.calls[1][1].body)).toEqual({ scope: 'project', projectPath: '/tmp/project', expectedRevision: 3, content: 'draft' });
        expect(useMemoryStore.getState()).toMatchObject({ content: 'normalized', revision: 4, dirty: false });
    });
    it('keeps the saved content and dirty state when the revision conflicts', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(response(doc('initial'))).mockResolvedValueOnce(response({}, 409)));
        await useMemoryStore.getState().loadFile(); useMemoryStore.getState().setDirty(true);
        expect(await useMemoryStore.getState().saveRaw('draft')).toBe(false);
        expect(useMemoryStore.getState()).toMatchObject({ content: 'initial', dirty: true, conflict: true, revision: 1 });
        expect(await useMemoryStore.getState().saveRaw('second')).toBe(false);
    });
    it('cannot mistake an invalid or failed save response for success and permits retry', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(response(doc('initial'))).mockResolvedValueOnce(response({ ok: true })).mockResolvedValueOnce(response(doc('retry', 2))));
        await useMemoryStore.getState().loadFile();
        expect(await useMemoryStore.getState().saveRaw('draft')).toBe(false);
        expect(useMemoryStore.getState().content).toBe('initial');
        expect(await useMemoryStore.getState().saveRaw('retry')).toBe(true);
    });
    it('ignores a late save after switching from global memory to another project', async () => {
        let finish!: (value: unknown) => void;
        vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(response(doc('global'))).mockImplementationOnce(() => new Promise(resolve => { finish = resolve; })).mockResolvedValueOnce(response(doc('project', 8))));
        await useMemoryStore.getState().loadFile(); const saving = useMemoryStore.getState().saveRaw('old save');
        await useMemoryStore.getState().setScope('project', '/tmp/project'); finish(response(doc('old save', 2)));
        expect(await saving).toBe(false);
        expect(useMemoryStore.getState()).toMatchObject({ scope: 'project', content: 'project', revision: 8 });
    });
    it('does not send saves when the document was never loaded', async () => {
        const fetchMock = vi.fn(); vi.stubGlobal('fetch', fetchMock);
        expect(await useMemoryStore.getState().saveEntries([])).toBe(false);
        expect(fetchMock).not.toHaveBeenCalled();
    });
});

it('adapts SQLite entry title and timestamps without dropping its identity or metadata', async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce(response({ ...doc('saved'), entries: [{ id: 'm', category: 'SEMANTIC', title: 'A title', content: 'old', source: 'USER', updatedAt: '2026-10-06', keywords: 'hint' }] })).mockResolvedValueOnce(response(doc('edited', 2)));
    vi.stubGlobal('fetch', fetchMock);
    await useMemoryStore.getState().loadFile();
    expect(useMemoryStore.getState().entries[0].timestamp).toBe('2026-10-06');
    await useMemoryStore.getState().saveEntries([...useMemoryStore.getState().entries, { category: 'semantic', content: 'new content', source: 'USER', timestamp: 'now' }]);
    const body = JSON.parse(fetchMock.mock.calls[1][1].body);
    expect(body.entries[0]).toEqual({ id: 'm', category: 'SEMANTIC', title: 'A title', content: 'old', source: 'USER', keywords: 'hint' });
    expect(body.entries[1]).toEqual({ category: 'semantic', title: 'new content', content: 'new content', source: 'USER', keywords: null });
});
