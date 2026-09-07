import { useConfigStore } from '@/store/configStore';
import { useProjectStore } from '@/store/projectStore';
import { useSessionStore } from '@/store/sessionStore';
import { useModelStore } from '@/store/modelStore';

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

async function selectAvailableModel(): Promise<string | null> {
    let catalog = useModelStore.getState();
    if (!catalog.loaded || catalog.loading || catalog.error) {
        await catalog.fetchModels();
        catalog = useModelStore.getState();
    }
    if (!catalog.loaded || catalog.error) {
        // 目录不可用时省略 model，让服务端裁定有效默认，避免发送本地陈旧值。
        return null;
    }

    const configuredDefault = useConfigStore.getState().defaultModel;
    return catalog.models.some(model => model.id === configuredDefault)
        ? configuredDefault
        : catalog.defaultModel;
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

        const selectedModel = await selectAvailableModel();
        const sessionState = useSessionStore.getState();
        return sessionState.createSession(
            project.id,
            selectedModel,
        );
    })().finally(() => {
        pendingCreation = null;
    });

    return pendingCreation;
}
