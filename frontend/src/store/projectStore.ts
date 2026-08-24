import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';

export interface Project {
    id: string;
    name: string;
    workspaceRoot: string;
    createdAt: string;
}

export interface ProjectStoreState {
    projects: Project[];
    loading: boolean;
    isOpen: boolean;
    requesting: boolean;
    revokingProjectId: string | null;
    error: string | null;
    loadProjects: () => Promise<void>;
    createProject: (
        name: string,
        workspaceRoot: string,
    ) => Promise<Project>;
    revokeProject: (projectId: string) => Promise<void>;
    requestSelection: () => Promise<Project | null>;
    confirmSelection: (project: Project) => void;
    cancelSelection: () => void;
}

let pendingSelection:
    | { resolve: (project: Project | null) => void }
    | null = null;
let nextLoadRequestId = 0;
let latestLoadRequestId = 0;
let projectMutationVersion = 0;

export async function projectApiErrorMessage(
    response: Response,
): Promise<string> {
    try {
        const body = await response.json() as {
            message?: string;
            error?: string | { message?: string };
        };
        if (typeof body.error === 'object') {
            return body.error.message ?? `HTTP ${response.status}`;
        }
        return body.message ?? body.error ?? `HTTP ${response.status}`;
    } catch {
        return `HTTP ${response.status}`;
    }
}

function isProject(value: unknown): value is Project {
    if (typeof value !== 'object' || value === null) return false;
    const candidate = value as Partial<Project>;
    return typeof candidate.id === 'string'
        && candidate.id.trim() !== ''
        && typeof candidate.name === 'string'
        && candidate.name.trim() !== ''
        && typeof candidate.workspaceRoot === 'string'
        && candidate.workspaceRoot.trim() !== ''
        && typeof candidate.createdAt === 'string';
}

export const useProjectStore = create<ProjectStoreState>()(
    immer((set, get) => ({
        projects: [],
        loading: false,
        isOpen: false,
        requesting: false,
        revokingProjectId: null,
        error: null,

        loadProjects: async () => {
            const requestId = ++nextLoadRequestId;
            latestLoadRequestId = requestId;
            const mutationVersion = projectMutationVersion;
            set(d => {
                d.loading = true;
                d.error = null;
            });
            try {
                const response = await fetch('/api/projects');
                if (!response.ok) {
                    throw new Error(
                        await projectApiErrorMessage(response));
                }
                const projects = await response.json() as unknown;
                if (!Array.isArray(projects)
                        || !projects.every(isProject)) {
                    throw new Error(
                        '服务端返回了无效的 Project 列表');
                }
                if (requestId === latestLoadRequestId
                        && mutationVersion === projectMutationVersion) {
                    set(d => { d.projects = projects; });
                }
            } catch (error) {
                if (requestId === latestLoadRequestId
                        && mutationVersion === projectMutationVersion) {
                    set(d => {
                        d.error = error instanceof Error
                            ? error.message : '加载 Project 失败';
                    });
                }
                throw error;
            } finally {
                if (requestId === latestLoadRequestId) {
                    set(d => { d.loading = false; });
                }
            }
        },

        createProject: async (name, workspaceRoot) => {
            set(d => {
                d.requesting = true;
                d.error = null;
            });
            try {
                const response = await fetch('/api/projects', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, workspaceRoot }),
                });
                if (!response.ok) {
                    throw new Error(
                        await projectApiErrorMessage(response));
                }
                const project = await response.json() as unknown;
                if (!isProject(project)) {
                    throw new Error(
                        '服务端返回了无效的 Project');
                }
                projectMutationVersion += 1;
                set(d => {
                    d.projects = [
                        project,
                        ...d.projects.filter(
                            item => item.id !== project.id),
                    ];
                });
                return project;
            } catch (error) {
                set(d => {
                    d.error = error instanceof Error
                        ? error.message : '创建 Project 失败';
                });
                throw error;
            } finally {
                set(d => { d.requesting = false; });
            }
        },

        revokeProject: async projectId => {
            if (!projectId.trim()) {
                throw new Error('Project ID 不能为空');
            }
            set(d => {
                d.revokingProjectId = projectId;
                d.error = null;
            });
            try {
                const response = await fetch(
                    `/api/projects/${encodeURIComponent(projectId)}`,
                    { method: 'DELETE' },
                );
                if (!response.ok) {
                    throw new Error(
                        await projectApiErrorMessage(response));
                }
                projectMutationVersion += 1;
                set(d => {
                    d.projects = d.projects.filter(
                        project => project.id !== projectId);
                });
            } catch (error) {
                set(d => {
                    d.error = error instanceof Error
                        ? error.message : '撤销 Project 授权失败';
                });
                throw error;
            } finally {
                set(d => { d.revokingProjectId = null; });
            }
        },

        requestSelection: async () => {
            if (pendingSelection || get().isOpen) return null;
            set(d => {
                d.isOpen = true;
                d.error = null;
            });
            void get().loadProjects().catch(() => undefined);
            return new Promise<Project | null>(resolve => {
                pendingSelection = { resolve };
            });
        },

        confirmSelection: project => {
            const pending = pendingSelection;
            pendingSelection = null;
            set(d => {
                d.isOpen = false;
                d.error = null;
            });
            pending?.resolve(project);
        },

        cancelSelection: () => {
            if (get().requesting || get().revokingProjectId) return;
            const pending = pendingSelection;
            pendingSelection = null;
            set(d => {
                d.isOpen = false;
                d.error = null;
            });
            pending?.resolve(null);
        },
    })),
);
