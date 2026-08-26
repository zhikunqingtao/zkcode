/**
 * McpCapabilityStore 契约归一化测试 — MCP 注册表 schema 1.1
 *
 * 覆盖：url/sseUrl 双键名归一（归一为 url、剔除 sseUrl）、
 * schema 1.1 新增字段（serverKey/transportType）透传不丢失、
 * PUT/POST 响应写回 state 前同样归一。
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useMcpCapabilityStore, type McpCapabilityDefinition } from '../mcpCapabilityStore';

/** 构造后端原始能力条目（可注入 url/sseUrl/schema 1.1 字段） */
function rawCapability(overrides: Record<string, unknown> = {}): Record<string, unknown> {
    return {
        id: 'cap-1',
        name: '图像生成',
        toolName: 'image_generation',
        apiKeyConfig: 'IMAGE_API_KEY',
        domain: 'image_processing',
        category: 'tool',
        briefDescription: '生成图像',
        description: '生成图像的工具',
        input: {},
        output: {},
        timeoutMs: 30000,
        enabled: true,
        videoCallEnabled: false,
        ...overrides,
    };
}

function stubFetchOnce(payload: unknown) {
    const fetchMock = vi.fn(async () => ({
        ok: true,
        status: 200,
        json: async () => payload,
    } as unknown as Response));
    vi.stubGlobal('fetch', fetchMock);
    return fetchMock;
}

describe('McpCapabilityStore schema 1.1 归一化', () => {
    beforeEach(() => {
        useMcpCapabilityStore.setState({
            capabilities: [],
            domains: [],
            activeDomain: null,
            loading: false,
            total: 0,
            enabledCount: 0,
            testResults: {},
        });
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
    });

    it('loadCapabilities 归一 url 键名并剔除旧 sseUrl 字段', async () => {
        stubFetchOnce({
            capabilities: [rawCapability({
                url: 'https://mcp.example.com/sse',
                sseUrl: 'https://legacy.example.com/sse',
            })],
            total: 1,
            enabledCount: 1,
        });

        await useMcpCapabilityStore.getState().loadCapabilities();

        const [cap] = useMcpCapabilityStore.getState().capabilities;
        expect(cap.url).toBe('https://mcp.example.com/sse');
        expect(cap).not.toHaveProperty('sseUrl');
    });

    it('loadCapabilities 兼容仅含旧 sseUrl 键名的条目', async () => {
        stubFetchOnce({
            capabilities: [rawCapability({ sseUrl: 'https://legacy.example.com/sse' })],
            total: 1,
            enabledCount: 1,
        });

        await useMcpCapabilityStore.getState().loadCapabilities();

        const [cap] = useMcpCapabilityStore.getState().capabilities;
        expect(cap.url).toBe('https://legacy.example.com/sse');
        expect(cap).not.toHaveProperty('sseUrl');
    });

    it('loadCapabilities 透传 schema 1.1 字段 serverKey/transportType 不丢失', async () => {
        stubFetchOnce({
            capabilities: [rawCapability({
                url: 'https://mcp.example.com/sse',
                serverKey: 'image-gen',
                transportType: 'streamable_http',
            })],
            total: 1,
            enabledCount: 1,
        });

        await useMcpCapabilityStore.getState().loadCapabilities();

        const [cap] = useMcpCapabilityStore.getState().capabilities;
        expect(cap.serverKey).toBe('image-gen');
        expect(cap.transportType).toBe('streamable_http');
    });

    it('updateCapability 响应写回 state 前完成归一', async () => {
        const existing = rawCapability({
            url: 'https://mcp.example.com/sse',
        }) as unknown as McpCapabilityDefinition;
        useMcpCapabilityStore.setState({ capabilities: [existing], total: 1 });
        stubFetchOnce(rawCapability({ sseUrl: 'https://legacy.example.com/sse' }));

        await useMcpCapabilityStore.getState().updateCapability('cap-1', existing);

        const [cap] = useMcpCapabilityStore.getState().capabilities;
        expect(cap.url).toBe('https://legacy.example.com/sse');
        expect(cap).not.toHaveProperty('sseUrl');
    });

    it('addCapability 响应写回 state 前完成归一', async () => {
        stubFetchOnce(rawCapability({
            id: 'cap-2',
            sseUrl: 'https://legacy.example.com/sse',
            serverKey: 'legacy-gen',
        }));

        const draft = rawCapability({
            id: 'cap-2',
            url: 'https://mcp.example.com/sse',
        }) as unknown as McpCapabilityDefinition;
        await useMcpCapabilityStore.getState().addCapability(draft);

        const state = useMcpCapabilityStore.getState();
        expect(state.capabilities).toHaveLength(1);
        expect(state.capabilities[0].url).toBe('https://legacy.example.com/sse');
        expect(state.capabilities[0].serverKey).toBe('legacy-gen');
        expect(state.capabilities[0]).not.toHaveProperty('sseUrl');
    });
});
