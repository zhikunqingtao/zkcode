import { create } from 'zustand';
import { useMcpStore } from './mcpStore';

export interface McpService {
    name: string;
    transport: string;
    scope: string;
    enabled: boolean;
    status: string;
    toolCount: number;
}

interface ServiceState {
    services: McpService[];
    loading: boolean;
    changing: string | null;
    error: string | null;
    fetchServices: () => Promise<void>;
    setEnabled: (name: string, enabled: boolean) => Promise<boolean>;
}

async function responseJson(response: Response) {
    if (!response.ok) {
        const payload = await response.json().catch(() => null);
        throw new Error(payload?.error?.message || payload?.message || `请求失败（HTTP ${response.status}）`);
    }
    return response.json();
}

function parseService(value: unknown): McpService {
    if (typeof value !== 'object' || value === null) throw new Error('服务状态响应无效');
    const service = value as Record<string, unknown>;
    if (typeof service.name !== 'string' || typeof service.transport !== 'string' ||
        typeof service.scope !== 'string' || typeof service.enabled !== 'boolean' ||
        typeof service.status !== 'string' || typeof service.toolCount !== 'number' ||
        !Number.isSafeInteger(service.toolCount) || service.toolCount < 0) throw new Error('服务状态响应无效');
    return service as unknown as McpService;
}

let revision = 0;
export const useMcpServiceStore = create<ServiceState>((set, get) => ({
    services: [], loading: false, changing: null, error: null,
    fetchServices: async () => {
        const current = ++revision;
        set({ loading: true, error: null });
        try {
            const result = await responseJson(await fetch('/api/mcp/services'));
            if (!Array.isArray(result.services)) throw new Error('服务列表响应无效');
            const services = result.services.map(parseService);
            if (current === revision) set({ services });
        } catch (error) {
            if (current === revision) set({ error: error instanceof Error ? error.message : '服务列表加载失败' });
        } finally {
            if (current === revision) set({ loading: false });
        }
    },
    setEnabled: async (name, enabled) => {
        if (get().changing !== null) return false;
        ++revision; // Earlier list requests may not overwrite this mutation.
        set({ changing: name, loading: false, error: null });
        try {
            const service = parseService(await responseJson(await fetch(`/api/mcp/services/${encodeURIComponent(name)}`, {
                method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ enabled }),
            })));
            if (service.name !== name) throw new Error('服务状态响应不匹配');
            set(state => ({ services: state.services.map(item => item.name === name ? service : item) }));
            if (!service.enabled) {
                useMcpStore.setState(state => {
                    const mcpTools = new Map(state.mcpTools); mcpTools.delete(name);
                    const resources = new Map(state.resources); resources.delete(name);
                    return { mcpTools, resources, prompts: state.prompts.filter(prompt => prompt.serverName !== name), selectedResource: null, resourceContent: null };
                });
            }
            return true;
        } catch (error) {
            // The server can have persisted enabled intent before a connection
            // fails. Reload observed state; never optimistically claim success.
            const message = error instanceof Error ? error.message : '服务状态保存失败';
            await get().fetchServices();
            set({ error: message });
            return false;
        } finally {
            set({ changing: null });
        }
    },
}));
