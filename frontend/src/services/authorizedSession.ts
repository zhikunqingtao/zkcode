import { useConfigStore } from '@/store/configStore';
import { useProjectStore } from '@/store/projectStore';
import { useSessionStore } from '@/store/sessionStore';

let pendingCreation: Promise<string | null> | null = null;

export const NEW_AUTHORIZED_SESSION_EVENT =
    'zkcode:new-authorized-session';

export function dispatchNewAuthorizedSessionRequest(): void {
    window.dispatchEvent(new Event(NEW_AUTHORIZED_SESSION_EVENT));
}

/**
 * Phase 1 Rust backend ships without the Project domain: GET /api/projects
 * answers 404 and POST /api/sessions accepts a Session without a Project
 * binding. In that case the chooser could never complete, so skip it and
 * create an unbound Session instead. Any other status or a network failure
 * keeps the normal authorization flow.
 */
async function projectDomainMissing(): Promise<boolean> {
    try {
        const response = await fetch('/api/projects');
        return response.status === 404;
    } catch {
        return false;
    }
}

/**
 * Opens the persistent Project authorization chooser and creates one Session
 * bound to the selected authorization. Concurrent callers share the same
 * chooser and Session POST so a double submit cannot create two Sessions.
 */
export function requestAuthorizedSession(): Promise<string | null> {
    if (pendingCreation) return pendingCreation;

    pendingCreation = (async () => {
        if (await projectDomainMissing()) {
            // Phase 1 后端同样没有 /api/config：本地兜底的 defaultModel
            // 可能不被 provider 认识，省略 model 交给服务端默认模型。
            return useSessionStore.getState().createSession(null, null);
        }
        const project = await useProjectStore.getState().requestSelection();
        if (!project) return null;

        const sessionState = useSessionStore.getState();
        const selectedModel = sessionState.model
            ?? useConfigStore.getState().defaultModel
            ?? 'qwen3.7-max';
        return sessionState.createSession(
            project.id,
            selectedModel,
        );
    })().finally(() => {
        pendingCreation = null;
    });

    return pendingCreation;
}
