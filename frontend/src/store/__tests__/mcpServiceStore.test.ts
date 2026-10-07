import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useMcpServiceStore, type McpService } from '../mcpServiceStore';
import { useMcpStore } from '../mcpStore';

const service: McpService = { name: 'alpha', transport: 'STDIO', scope: 'USER', enabled: true, status: 'connected', toolCount: 2 };
const response = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } });

describe('MCP service controls', () => {
    beforeEach(() => {
        useMcpServiceStore.setState({ services: [service], loading: false, changing: null, error: null });
        useMcpStore.setState({ mcpTools: new Map(), resources: new Map(), prompts: [] });
    });
    afterEach(() => vi.unstubAllGlobals());

    it('preserves a disabled service in the list, including actual failed connection states', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({ services: [{ ...service, enabled: false, status: 'disabled', toolCount: 0 }, { ...service, name: 'beta', status: 'failed', toolCount: 0 }] })));
        await useMcpServiceStore.getState().fetchServices();
        expect(useMcpServiceStore.getState().services.map(item => [item.name, item.enabled, item.status])).toEqual([['alpha', false, 'disabled'], ['beta', true, 'failed']]);
    });

    it('waits for persisted state and removes only the disabled service discovery', async () => {
        let finish!: (response: Response) => void;
        const fetch = vi.fn(() => new Promise<Response>(resolve => { finish = resolve; }));
        vi.stubGlobal('fetch', fetch);
        useMcpStore.setState({ mcpTools: new Map([['alpha', []], ['beta', []]]), resources: new Map([['alpha', []], ['beta', []]]), prompts: [{ name: 'review', serverName: 'alpha', description: '', arguments: [] }, { name: 'other', serverName: 'beta', description: '', arguments: [] }] });
        const pending = useMcpServiceStore.getState().setEnabled('alpha', false);
        expect(useMcpServiceStore.getState().services[0].enabled).toBe(true);
        expect(await useMcpServiceStore.getState().setEnabled('beta', false)).toBe(false);
        finish(response({ ...service, enabled: false, status: 'disabled', toolCount: 0 }));
        expect(await pending).toBe(true);
        expect(fetch).toHaveBeenCalledWith('/api/mcp/services/alpha', expect.objectContaining({ method: 'PATCH', body: '{"enabled":false}' }));
        expect([...useMcpStore.getState().mcpTools.keys()]).toEqual(['beta']);
        expect([...useMcpStore.getState().resources.keys()]).toEqual(['beta']);
        expect(useMcpStore.getState().prompts.map(item => item.serverName)).toEqual(['beta']);
    });

    it('reloads authoritative state after a storage or connection failure and keeps the error visible', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(response({ code: 'MCP_SERVICE_STORAGE_UNAVAILABLE', message: '保存失败' }, 503)).mockResolvedValueOnce(response({ services: [service] })));
        expect(await useMcpServiceStore.getState().setEnabled('alpha', false)).toBe(false);
        expect(useMcpServiceStore.getState().services[0].enabled).toBe(true);
        expect(useMcpServiceStore.getState().error).toBe('保存失败');
        expect(useMcpServiceStore.getState().changing).toBeNull();
    });

    it('does not let an older list response undo the new persisted preference', async () => {
        let finish!: (response: Response) => void;
        vi.stubGlobal('fetch', vi.fn().mockImplementationOnce(() => new Promise<Response>(resolve => { finish = resolve; })).mockResolvedValueOnce(response({ ...service, enabled: false, status: 'disabled', toolCount: 0 })));
        const old = useMcpServiceStore.getState().fetchServices();
        await useMcpServiceStore.getState().setEnabled('alpha', false);
        finish(response({ services: [service] }));
        await old;
        expect(useMcpServiceStore.getState().services[0].enabled).toBe(false);
        expect(useMcpServiceStore.getState().loading).toBe(false);
    });

    it('rejects malformed service responses without losing the latest valid list', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({ services: [{ ...service, enabled: 'false' }] })));
        await useMcpServiceStore.getState().fetchServices();
        expect(useMcpServiceStore.getState().services).toEqual([service]);
        expect(useMcpServiceStore.getState().error).toBe('服务状态响应无效');
    });
});
