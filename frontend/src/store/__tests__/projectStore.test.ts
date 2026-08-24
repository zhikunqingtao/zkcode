import { waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useProjectStore, type Project } from '../projectStore';

const project: Project = {
    id: 'project-1',
    name: 'Demo',
    workspaceRoot: '/workspace/demo',
    createdAt: '2026-07-30T00:00:00Z',
};

describe('ProjectStore', () => {
    beforeEach(() => {
        useProjectStore.getState().cancelSelection();
        useProjectStore.setState({
            projects: [],
            loading: false,
            isOpen: false,
            requesting: false,
            revokingProjectId: null,
            error: null,
        });
    });

    afterEach(() => {
        useProjectStore.getState().cancelSelection();
        vi.unstubAllGlobals();
    });

    it('loads only the minimal Project list contract', async () => {
        const fetchMock = vi.fn().mockResolvedValue({
            ok: true,
            json: async () => [project],
        });
        vi.stubGlobal('fetch', fetchMock);

        await useProjectStore.getState().loadProjects();

        expect(fetchMock).toHaveBeenCalledTimes(1);
        expect(fetchMock).toHaveBeenCalledWith('/api/projects');
        expect(useProjectStore.getState().projects).toEqual([project]);
        expect(useProjectStore.getState().loading).toBe(false);
        expect(useProjectStore.getState().error).toBeNull();
    });

    it('opens one selection request and resolves it with the chosen Project', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
            ok: true,
            json: async () => [project],
        }));

        const selection = useProjectStore.getState().requestSelection();

        expect(useProjectStore.getState().isOpen).toBe(true);
        await waitFor(() => {
            expect(useProjectStore.getState().projects)
                .toEqual([project]);
        });
        await expect(useProjectStore.getState().requestSelection())
            .resolves.toBeNull();

        useProjectStore.getState().confirmSelection(project);

        await expect(selection).resolves.toEqual(project);
        expect(useProjectStore.getState().isOpen).toBe(false);
    });

    it('creates a Project with only name and workspaceRoot', async () => {
        const fetchMock = vi.fn().mockResolvedValue({
            ok: true,
            status: 201,
            json: async () => project,
        });
        vi.stubGlobal('fetch', fetchMock);

        await expect(useProjectStore.getState().createProject(
            'Demo', '/workspace/demo'))
            .resolves.toEqual(project);

        expect(fetchMock).toHaveBeenCalledWith('/api/projects', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: 'Demo',
                workspaceRoot: '/workspace/demo',
            }),
        });
        expect(useProjectStore.getState().projects).toEqual([project]);
    });

    it('does not let an older list response erase a newly created Project', async () => {
        let resolveList!: (response: Response) => void;
        const pendingList = new Promise<Response>(resolve => {
            resolveList = resolve;
        });
        const created: Project = {
            ...project,
            id: 'project-new',
            name: 'New Project',
        };
        const fetchMock = vi.fn()
            .mockReturnValueOnce(pendingList)
            .mockResolvedValueOnce({
                ok: true,
                status: 201,
                json: async () => created,
            });
        vi.stubGlobal('fetch', fetchMock);

        const loading = useProjectStore.getState().loadProjects();
        await useProjectStore.getState().createProject(
            created.name,
            created.workspaceRoot,
        );
        resolveList({
            ok: true,
            status: 200,
            json: async () => [],
        } as Response);
        await loading;

        expect(useProjectStore.getState().projects).toEqual([created]);
    });

    it('revokes persistent authorization through the Project endpoint', async () => {
        useProjectStore.setState({ projects: [project] });
        const fetchMock = vi.fn().mockResolvedValue({
            ok: true,
            status: 204,
        });
        vi.stubGlobal('fetch', fetchMock);

        await useProjectStore.getState().revokeProject(project.id);

        expect(fetchMock).toHaveBeenCalledWith(
            '/api/projects/project-1',
            { method: 'DELETE' },
        );
        expect(useProjectStore.getState().projects).toEqual([]);
        expect(useProjectStore.getState().revokingProjectId).toBeNull();
    });

    it('rejects an invalid list without replacing trusted state', async () => {
        useProjectStore.setState({ projects: [project] });
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
            ok: true,
            json: async () => [{
                id: '',
                name: 'Invalid',
                workspaceRoot: '/workspace/invalid',
            }],
        }));

        await expect(useProjectStore.getState().loadProjects())
            .rejects.toThrow('无效的 Project 列表');

        expect(useProjectStore.getState().projects).toEqual([project]);
        expect(useProjectStore.getState().loading).toBe(false);
        expect(useProjectStore.getState().error)
            .toBe('服务端返回了无效的 Project 列表');
    });
});
