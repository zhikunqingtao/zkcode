import {
    act,
    cleanup,
    fireEvent,
    render,
    screen,
    waitFor,
} from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ProjectSelectionDialog } from './ProjectSelectionDialog';
import { useProjectStore, type Project } from '@/store/projectStore';

const project: Project = {
    id: 'project-1',
    name: 'Demo',
    workspaceRoot: '/workspace/demo',
    createdAt: '2026-07-30T00:00:00Z',
};

interface DirectoryListingResponse {
    roots: string[];
    current: string;
    parent?: string | null;
    directories: Array<{ name: string; path: string }>;
    nativePickerAvailable?: boolean;
}

const directoryListing: DirectoryListingResponse = {
    roots: ['/workspace'],
    current: '/workspace',
    parent: null,
    directories: [{ name: 'demo', path: '/workspace/demo' }],
};

function directoryResponse(
    listing: DirectoryListingResponse = directoryListing,
) {
    return {
        ok: true,
        status: 200,
        json: async () => listing,
    };
}

const storeActions = {
    loadProjects: useProjectStore.getState().loadProjects,
    createProject: useProjectStore.getState().createProject,
    revokeProject: useProjectStore.getState().revokeProject,
    requestSelection: useProjectStore.getState().requestSelection,
    confirmSelection: useProjectStore.getState().confirmSelection,
    cancelSelection: useProjectStore.getState().cancelSelection,
};

describe('ProjectSelectionDialog', () => {
    beforeEach(() => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(
            directoryResponse(),
        ));
        useProjectStore.setState({
            ...storeActions,
            projects: [project],
            loading: false,
            isOpen: true,
            requesting: false,
            revokingProjectId: null,
            error: null,
        });
    });

    afterEach(() => {
        cleanup();
        useProjectStore.setState({
            ...storeActions,
            projects: [],
            isOpen: false,
            requesting: false,
            revokingProjectId: null,
            error: null,
        });
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
    });

    it('explains persistent authorization and confirms an existing Project', async () => {
        const confirmSelection = vi.fn();
        useProjectStore.setState({ confirmSelection });

        render(<ProjectSelectionDialog />);

        expect(screen.getByText(
            /Project 会把所选服务端目录持久注册为 Session 的默认相对路径根和信任范围/,
        )).toBeInTheDocument();
        expect(screen.getByText(
            /目录内普通读写无需重复确认/,
        )).toBeInTheDocument();
        expect(screen.getByText(
            /目录外的普通文件操作会请求授权，并可按本次运行或会话记住/,
        )).toBeInTheDocument();
        expect(screen.getByText(
            /敏感文件和高风险操作每次都需确认/,
        )).toBeInTheDocument();
        expect(screen.getByText(
            /不会上传浏览器本地文件/,
        )).toBeInTheDocument();
        await screen.findByRole('button', {
            name: '打开目录 demo',
        });

        const confirm = screen.getByRole('button', {
            name: '使用所选授权',
        });
        expect(confirm).toBeDisabled();
        fireEvent.click(screen.getByRole('radio'));
        expect(confirm).toBeEnabled();
        fireEvent.click(confirm);

        expect(confirmSelection).toHaveBeenCalledWith(project);
    });

    it('accepts a root directory response that omits the null parent', async () => {
        const rootProject: Project = {
            ...project,
            id: 'project-root',
            name: 'workspace',
            workspaceRoot: '/workspace',
        };
        vi.mocked(fetch).mockResolvedValueOnce(directoryResponse({
            roots: ['/workspace'],
            current: '/workspace',
            directories: [{ name: 'demo', path: '/workspace/demo' }],
        }) as Response);
        const createProject = vi.fn(async () => rootProject);
        useProjectStore.setState({
            projects: [],
            createProject,
        });

        render(<ProjectSelectionDialog />);

        expect(await screen.findByRole('button', {
            name: '打开目录 demo',
        })).toBeInTheDocument();
        expect(screen.queryByRole('alert')).not.toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', {
            name: '授权此文件夹',
        }));

        await waitFor(() => {
            expect(createProject).toHaveBeenCalledWith(
                'workspace', '/workspace');
        });
    });

    it('authorizes the browsed server directory before confirming it', async () => {
        const created: Project = {
            ...project,
            id: 'project-created',
            name: 'Created',
            workspaceRoot: '/workspace/created',
        };
        vi.mocked(fetch).mockResolvedValueOnce(directoryResponse({
            ...directoryListing,
            current: '/workspace/created',
            directories: [],
        }) as Response);
        const createProject = vi.fn(async () => {
            useProjectStore.setState({
                projects: [created, project],
            });
            return created;
        });
        const confirmSelection = vi.fn();
        useProjectStore.setState({
            createProject,
            confirmSelection,
        });

        render(<ProjectSelectionDialog />);

        const nameInput = await screen.findByRole('textbox', {
            name: '授权名称',
        });
        fireEvent.change(nameInput, {
            target: { value: '  Created  ' },
        });
        fireEvent.click(screen.getByRole('button', {
            name: '授权此文件夹',
        }));

        await waitFor(() => {
            expect(createProject).toHaveBeenCalledWith(
                'Created', '/workspace/created');
        });
        const confirm = screen.getByRole('button', {
            name: '使用所选授权',
        });
        await waitFor(() => expect(confirm).toBeEnabled());
        fireEvent.click(confirm);

        expect(confirmSelection).toHaveBeenCalledWith(created);
    });

    it('navigates with the server directory API', async () => {
        const fetchMock = vi.mocked(fetch);
        fetchMock.mockResolvedValueOnce(directoryResponse() as Response)
            .mockResolvedValueOnce(directoryResponse({
                roots: ['/workspace'],
                current: '/workspace/demo',
                parent: '/workspace',
                directories: [],
            }) as Response);

        render(<ProjectSelectionDialog />);
        fireEvent.click(await screen.findByRole('button', {
            name: '打开目录 demo',
        }));

        await waitFor(() => {
            expect(fetchMock).toHaveBeenLastCalledWith(
                '/api/projects/directories?path=%2Fworkspace%2Fdemo',
            );
        });
        expect(await screen.findByText('/workspace/demo', {
            selector: 'span',
        }))
            .toBeInTheDocument();
        expect(screen.getByRole('button', {
            name: '打开上一级目录',
        })).toBeInTheDocument();
    });

    it('can navigate above the initial local default directory', async () => {
        const fetchMock = vi.mocked(fetch);
        fetchMock.mockResolvedValueOnce(directoryResponse({
            roots: ['/'],
            current: '/workspace/repo',
            parent: '/workspace',
            directories: [],
        }) as Response).mockResolvedValueOnce(directoryResponse({
            roots: ['/'],
            current: '/workspace',
            parent: '/',
            directories: [{
                name: 'sibling',
                path: '/workspace/sibling',
            }],
        }) as Response);

        render(<ProjectSelectionDialog />);
        fireEvent.click(await screen.findByRole('button', {
            name: '打开上一级目录',
        }));

        await waitFor(() => {
            expect(fetchMock).toHaveBeenLastCalledWith(
                '/api/projects/directories?path=%2Fworkspace',
            );
        });
        expect(await screen.findByRole('button', {
            name: '打开目录 sibling',
        })).toBeInTheDocument();
    });

    it('shows the native picker only when available and applies its selection', async () => {
        const pickedProject: Project = {
            ...project,
            id: 'project-picked',
            name: 'Picked',
            workspaceRoot: '/workspace/picked',
        };
        const fetchMock = vi.mocked(fetch);
        fetchMock.mockResolvedValueOnce(directoryResponse({
            ...directoryListing,
            nativePickerAvailable: true,
        }) as Response).mockResolvedValueOnce(directoryResponse({
            roots: ['/'],
            current: '/workspace/picked',
            parent: '/workspace',
            directories: [],
            nativePickerAvailable: true,
        }) as Response);
        useProjectStore.setState({
            projects: [project, pickedProject],
        });

        render(<ProjectSelectionDialog />);

        const picker = await screen.findByRole('button', {
            name: '选择本机文件夹…',
        });
        fireEvent.click(picker);

        await waitFor(() => {
            expect(fetchMock).toHaveBeenLastCalledWith(
                '/api/projects/directories/pick',
                {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Zhikun-Native-Picker': '1',
                    },
                    body: '{}',
                },
            );
        });
        expect(await screen.findByDisplayValue(
            '/workspace/picked')).toBeInTheDocument();
        expect(screen.getByDisplayValue('picked')).toBeInTheDocument();
        expect(screen.getByRole('radio', {
            name: /Picked/,
        })).toBeChecked();
        expect(screen.getByRole('button', {
            name: '使用所选授权',
        })).toBeEnabled();
    });

    it('does not show the native picker when the server omits capability', async () => {
        render(<ProjectSelectionDialog />);

        await screen.findByRole('button', {
            name: '打开目录 demo',
        });
        expect(screen.queryByRole('button', {
            name: '选择本机文件夹…',
        })).not.toBeInTheDocument();
    });

    it('keeps the current folder when the native picker is cancelled', async () => {
        const fetchMock = vi.mocked(fetch);
        fetchMock.mockResolvedValueOnce(directoryResponse({
            ...directoryListing,
            nativePickerAvailable: true,
        }) as Response).mockResolvedValueOnce({
            ok: true,
            status: 204,
        } as Response);

        render(<ProjectSelectionDialog />);

        fireEvent.click(await screen.findByRole('button', {
            name: '选择本机文件夹…',
        }));

        await waitFor(() => {
            expect(screen.getByRole('button', {
                name: '选择本机文件夹…',
            })).toBeEnabled();
        });
        expect(screen.getByDisplayValue('/workspace')).toBeInTheDocument();
        expect(screen.getByDisplayValue('workspace')).toBeInTheDocument();
    });

    it('renders native picker errors without changing the current folder', async () => {
        vi.mocked(fetch).mockResolvedValueOnce(directoryResponse({
            ...directoryListing,
            nativePickerAvailable: true,
        }) as Response).mockResolvedValueOnce({
            ok: false,
            status: 503,
            json: async () => ({
                message: '当前环境无法打开系统文件夹选择器',
            }),
        } as Response);

        render(<ProjectSelectionDialog />);
        fireEvent.click(await screen.findByRole('button', {
            name: '选择本机文件夹…',
        }));

        expect(await screen.findByRole('alert')).toHaveTextContent(
            '当前环境无法打开系统文件夹选择器');
        expect(screen.getByDisplayValue('/workspace')).toBeInTheDocument();
    });

    it('ignores a duplicate click and a picker response from a closed dialog', async () => {
        let resolvePicker!: (response: Response) => void;
        const fetchMock = vi.mocked(fetch);
        fetchMock.mockResolvedValueOnce(directoryResponse({
            ...directoryListing,
            nativePickerAvailable: true,
        }) as Response).mockReturnValueOnce(new Promise<Response>(resolve => {
            resolvePicker = resolve;
        })).mockResolvedValueOnce(directoryResponse({
            ...directoryListing,
            current: '/workspace/reopened',
            nativePickerAvailable: true,
        }) as Response);

        render(<ProjectSelectionDialog />);
        fireEvent.click(await screen.findByRole('button', {
            name: '选择本机文件夹…',
        }));
        const pendingButton = await screen.findByRole('button', {
            name: '正在打开文件夹选择器…',
        });
        fireEvent.click(pendingButton);
        expect(fetchMock).toHaveBeenCalledTimes(2);

        await act(async () => {
            useProjectStore.setState({ isOpen: false });
        });
        await act(async () => {
            useProjectStore.setState({ isOpen: true });
        });
        expect(await screen.findByText('/workspace/reopened', {
            selector: 'span',
        })).toBeInTheDocument();

        await act(async () => {
            resolvePicker(directoryResponse({
                ...directoryListing,
                current: '/workspace/late-picker',
                nativePickerAvailable: true,
            }) as Response);
            await Promise.resolve();
        });

        expect(screen.getByText('/workspace/reopened', {
            selector: 'span',
        })).toBeInTheDocument();
        expect(screen.queryByText('/workspace/late-picker', {
            selector: 'span',
        })).not.toBeInTheDocument();
    });

    it('clears an old Project selection after browsing another folder', async () => {
        const fetchMock = vi.mocked(fetch);
        fetchMock.mockResolvedValueOnce(directoryResponse() as Response)
            .mockResolvedValueOnce(directoryResponse({
                roots: ['/workspace'],
                current: '/workspace/other',
                parent: '/workspace',
                directories: [],
            }) as Response);

        render(<ProjectSelectionDialog />);
        fireEvent.click(screen.getByRole('radio'));
        expect(screen.getByRole('button', {
            name: '使用所选授权',
        })).toBeEnabled();

        fireEvent.click(await screen.findByRole('button', {
            name: '打开目录 demo',
        }));

        await screen.findByText('/workspace/other', {
            selector: 'span',
        });
        expect(screen.getByRole('button', {
            name: '使用所选授权',
        })).toBeDisabled();
    });

    it('does not let a late browse response overwrite manual input', async () => {
        let resolveDirectory!: (response: Response) => void;
        vi.mocked(fetch).mockReturnValueOnce(new Promise<Response>(resolve => {
            resolveDirectory = resolve;
        }));

        render(<ProjectSelectionDialog />);
        fireEvent.click(screen.getByText(
            '高级：手动输入服务端绝对路径',
        ));
        const pathInput = screen.getByRole('textbox', {
            name: '服务端绝对路径',
        });
        const nameInput = screen.getByRole('textbox', {
            name: '授权名称',
        });
        fireEvent.change(pathInput, {
            target: { value: '/workspace/manual' },
        });
        fireEvent.change(nameInput, {
            target: { value: 'Manual Project' },
        });

        await act(async () => {
            resolveDirectory(directoryResponse({
                ...directoryListing,
                current: '/workspace/late',
            }) as Response);
            await Promise.resolve();
            await Promise.resolve();
        });

        expect(pathInput).toHaveValue('/workspace/manual');
        expect(nameInput).toHaveValue('Manual Project');
    });

    it('revokes persistent authorization without claiming to stop Sessions', async () => {
        const revokeProject = vi.fn(async () => {
            useProjectStore.setState({ projects: [] });
        });
        useProjectStore.setState({ revokeProject });
        vi.spyOn(window, 'confirm').mockReturnValue(true);

        render(<ProjectSelectionDialog />);
        await screen.findByRole('button', { name: '打开目录 demo' });
        fireEvent.click(screen.getByRole('button', {
            name: '撤销 Demo 的持久自动编辑授权',
        }));

        await waitFor(() => {
            expect(revokeProject).toHaveBeenCalledWith(project.id);
        });
        expect(window.confirm).toHaveBeenCalledWith(
            expect.stringContaining('已有 Session 不会被终止'),
        );
    });

    it('renders a server validation error without closing the dialog', async () => {
        useProjectStore.setState({
            error: 'workspaceRoot is outside the allowed roots',
        });

        render(<ProjectSelectionDialog />);

        expect(screen.getByRole('alert')).toHaveTextContent(
            'workspaceRoot is outside the allowed roots');
        expect(screen.getByText('选择文件夹授权')).toBeInTheDocument();
        await screen.findByRole('button', { name: '打开目录 demo' });
    });
});
