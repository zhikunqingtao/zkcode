import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useConfigStore } from '@/store/configStore';
import { useProjectStore, type Project } from '@/store/projectStore';
import { useSessionStore } from '@/store/sessionStore';
import { useModelStore } from '@/store/modelStore';
import { requestAuthorizedSession } from './authorizedSession';

const project: Project = {
    id: 'project-1',
    name: 'Demo',
    workspaceRoot: '/workspace/demo',
    createdAt: '2026-07-30T00:00:00Z',
};

const originalRequestSelection =
    useProjectStore.getState().requestSelection;
const originalCreateSession = useSessionStore.getState().createSession;
const originalFetchModels = useModelStore.getState().fetchModels;

describe('requestAuthorizedSession', () => {
    beforeEach(() => {
        useConfigStore.setState({ defaultModel: 'model-default' });
        useProjectStore.setState({
            requestSelection: originalRequestSelection,
        });
        useSessionStore.setState({
            sessionId: null,
            model: null,
            createSession: originalCreateSession,
        });
        useModelStore.setState({
            models: [model('model-default')],
            defaultModel: 'model-default',
            loaded: true,
            loading: false,
            error: null,
            fetchModels: originalFetchModels,
        });
    });

    afterEach(() => {
        useProjectStore.setState({
            requestSelection: originalRequestSelection,
        });
        useSessionStore.setState({
            sessionId: null,
            model: null,
            createSession: originalCreateSession,
        });
        useModelStore.setState({ fetchModels: originalFetchModels });
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
    });

    it('creates a Session only after a Project is selected', async () => {
        const requestSelection = vi.fn().mockResolvedValue(project);
        const createSession = vi.fn()
            .mockResolvedValue('session-created');
        useProjectStore.setState({ requestSelection });
        useSessionStore.setState({ createSession });

        await expect(requestAuthorizedSession())
            .resolves.toBe('session-created');

        expect(requestSelection).toHaveBeenCalledTimes(1);
        expect(createSession).toHaveBeenCalledWith(
            project.id,
            'model-default',
        );
    });

    it('replaces stale local choices with the backend catalog default', async () => {
        const requestSelection = vi.fn().mockResolvedValue(project);
        const createSession = vi.fn().mockResolvedValue('session-created');
        useConfigStore.setState({ defaultModel: 'retired-config-model' });
        useProjectStore.setState({ requestSelection });
        useSessionStore.setState({ model: 'retired-session-model', createSession });
        useModelStore.setState({
            models: [model('backend-current-model'), model('qwen3.8-max')],
            defaultModel: 'backend-current-model',
            loaded: true,
        });

        await expect(requestAuthorizedSession()).resolves.toBe('session-created');

        expect(createSession).toHaveBeenCalledWith(project.id, 'backend-current-model');
    });

    it('loads the catalog on a cold start before validating a stale model', async () => {
        const fetchMock = vi.fn((input: RequestInfo | URL) => {
            const url = String(input);
            if (url === '/api/projects') {
                return Promise.resolve({ ok: true, status: 200 } as Response);
            }
            if (url === '/api/models') {
                return Promise.resolve({
                    ok: true,
                    status: 200,
                    json: async () => ({
                        models: [model('cold-start-default')],
                        defaultModel: 'cold-start-default',
                    }),
                } as Response);
            }
            throw new Error(`unexpected fetch: ${url}`);
        });
        vi.stubGlobal('fetch', fetchMock);
        const requestSelection = vi.fn().mockResolvedValue(project);
        const createSession = vi.fn().mockResolvedValue('session-created');
        useProjectStore.setState({ requestSelection });
        useSessionStore.setState({ model: 'retired-model', createSession });
        useModelStore.setState({
            models: [],
            defaultModel: null,
            loaded: false,
            loading: false,
            error: null,
        });

        await expect(requestAuthorizedSession()).resolves.toBe('session-created');

        expect(fetchMock).toHaveBeenCalledWith('/api/models');
        expect(createSession).toHaveBeenCalledWith(project.id, 'cold-start-default');
    });

    it('fails closed when refreshing a stale cached catalog still fails', async () => {
        const fetchMock = vi.fn((input: RequestInfo | URL) => {
            const url = String(input);
            if (url === '/api/projects') {
                return Promise.resolve({ ok: true, status: 200 } as Response);
            }
            if (url === '/api/models') {
                return Promise.resolve({ ok: false, status: 503 } as Response);
            }
            throw new Error(`unexpected fetch: ${url}`);
        });
        vi.stubGlobal('fetch', fetchMock);
        const requestSelection = vi.fn().mockResolvedValue(project);
        const createSession = vi.fn().mockResolvedValue('session-created');
        useProjectStore.setState({ requestSelection });
        useSessionStore.setState({ model: 'old-model', createSession });
        useModelStore.setState({
            models: [model('old-model')],
            defaultModel: 'old-model',
            loaded: true,
            loading: false,
            error: 'previous refresh failed',
            fetchModels: originalFetchModels,
        });

        await expect(requestAuthorizedSession()).resolves.toBe('session-created');

        expect(createSession).toHaveBeenCalledWith(project.id, null);
        expect(useModelStore.getState().models).toEqual([]);
    });

    it('does not create a Session when folder selection is canceled', async () => {
        const requestSelection = vi.fn().mockResolvedValue(null);
        const createSession = vi.fn();
        useProjectStore.setState({ requestSelection });
        useSessionStore.setState({ createSession });

        await expect(requestAuthorizedSession()).resolves.toBeNull();

        expect(createSession).not.toHaveBeenCalled();
    });

    it('shares one authorization and Session request across double sends', async () => {
        // Project 域探测返回非 404 → 走正常授权选择流程。
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
            ok: true,
            status: 200,
        }));
        let resolveSelection!: (selection: Project | null) => void;
        const selection = new Promise<Project | null>(resolve => {
            resolveSelection = resolve;
        });
        const requestSelection = vi.fn(() => selection);
        const createSession = vi.fn()
            .mockResolvedValue('session-created');
        useProjectStore.setState({ requestSelection });
        useSessionStore.setState({ createSession });

        const first = requestAuthorizedSession();
        const second = requestAuthorizedSession();

        expect(second).toBe(first);
        // 探测为异步步骤，等待微任务队列排空后 chooser 才被打开。
        await new Promise(resolve => setTimeout(resolve, 0));
        expect(requestSelection).toHaveBeenCalledTimes(1);
        resolveSelection(project);
        await expect(Promise.all([first, second])).resolves.toEqual([
            'session-created',
            'session-created',
        ]);
        expect(createSession).toHaveBeenCalledTimes(1);
    });

    it('skips the chooser and creates an unbound Session when the Project domain is missing', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
            ok: false,
            status: 404,
        }));
        const requestSelection = vi.fn();
        const createSession = vi.fn()
            .mockResolvedValue('session-unbound');
        useProjectStore.setState({ requestSelection });
        useSessionStore.setState({ createSession });

        await expect(requestAuthorizedSession())
            .resolves.toBe('session-unbound');

        expect(requestSelection).not.toHaveBeenCalled();
        expect(createSession).toHaveBeenCalledWith(null, null);
        vi.unstubAllGlobals();
    });
});

function model(id: string) {
    return {
        id,
        displayName: id,
        supportsImages: false,
        maxImages: 0,
    };
}
