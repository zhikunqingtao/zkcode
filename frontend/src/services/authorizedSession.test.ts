import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useConfigStore } from '@/store/configStore';
import { useProjectStore, type Project } from '@/store/projectStore';
import { useSessionStore } from '@/store/sessionStore';
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

    it('prefers the model selected in Settings over the configured default', async () => {
        const requestSelection = vi.fn().mockResolvedValue(project);
        const createSession = vi.fn().mockResolvedValue('session-created');
        useProjectStore.setState({ requestSelection });
        useSessionStore.setState({
            model: 'qwen3.8-max',
            createSession,
        });

        await expect(requestAuthorizedSession()).resolves.toBe('session-created');

        expect(createSession).toHaveBeenCalledWith(project.id, 'qwen3.8-max');
    });

    it('falls back to qwen3.8-max when no selected or configured model exists', async () => {
        const requestSelection = vi.fn().mockResolvedValue(project);
        const createSession = vi.fn().mockResolvedValue('session-created');
        useConfigStore.setState({ defaultModel: null as unknown as string });
        useProjectStore.setState({ requestSelection });
        useSessionStore.setState({ model: null, createSession });

        await expect(requestAuthorizedSession()).resolves.toBe('session-created');

        expect(createSession).toHaveBeenCalledWith(project.id, 'qwen3.8-max');
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
