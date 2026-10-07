import { useProjectStore } from '@/store/projectStore';
import { useSessionStore } from '@/store/sessionStore';
import { ensurePythonPanelResponse } from './pythonServiceError';

interface AnalysisScope {
    projectRoot?: string;
    projectId?: string;
    sessionId?: string;
    requestId: string;
}

/** Captures authority at dispatch; a late response can never follow a session switch. */
export class AnalysisRequest {
    readonly controller = new AbortController();
    private readonly scope: AnalysisScope;
    private readonly headers: Record<string, string>;
    private finished = false;
    private unsubscribeProject: (() => void) | null = null;

    constructor(projectRoot: string, private readonly cancelPath = '/api/code-analysis/cancel') {
        const root = projectRoot.trim();
        const project = root && root !== '.'
            ? useProjectStore.getState().projects.find(item => item.workspaceRoot === root)
            : undefined;
        const sessionId = project ? undefined : useSessionStore.getState().sessionId ?? undefined;
        if (!project && !sessionId) throw new Error('请先选择已授权的项目或会话');
        this.scope = { projectRoot: root || undefined, projectId: project?.id, sessionId, requestId: crypto.randomUUID() };
        this.headers = { 'Content-Type': 'application/json', ...(sessionId ? { 'X-Session-Id': sessionId } : {}) };
        if (project) {
            this.unsubscribeProject = useProjectStore.subscribe(state => {
                if (!state.projects.some(item => item.id === project.id && item.workspaceRoot === project.workspaceRoot)) this.cancel();
            });
        }
    }

    async post<T>(path: string, payload: object): Promise<T> {
        try {
            const response = await fetch(path, {
                method: 'POST', headers: this.headers,
                body: JSON.stringify({ ...payload, ...this.scope }), signal: this.controller.signal,
            });
            await ensurePythonPanelResponse(response);
            const result: unknown = await response.json();
            if (!result || typeof result !== 'object' || ('success' in result && result.success === false) || ('error' in result && result.error)) {
                throw new Error('分析服务返回无效结果');
            }
            if (this.controller.signal.aborted) throw new DOMException('Analysis cancelled', 'AbortError');
            return result as T;
        } finally {
            this.finished = true;
            this.unsubscribeProject?.();
            this.unsubscribeProject = null;
        }
    }

    cancel(): void {
        if (this.finished || this.controller.signal.aborted) return;
        this.controller.abort();
        this.unsubscribeProject?.();
        this.unsubscribeProject = null;
        // Explicit cancellation reaches the worker even if an HTTP server continues
        // handling a request after the browser has closed its response connection.
        void fetch(this.cancelPath, {
            method: 'POST', headers: this.headers, body: JSON.stringify(this.scope), keepalive: true,
        }).catch(() => undefined); // The worker also owns a hard execution deadline.
    }
}

export function isAnalysisCancelled(error: unknown): boolean {
    return error instanceof DOMException && error.name === 'AbortError';
}
