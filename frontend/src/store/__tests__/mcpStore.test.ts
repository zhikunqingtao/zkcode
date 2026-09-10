import { beforeEach, describe, expect, it } from 'vitest';
import { useMcpStore } from '../mcpStore';

describe('McpStore progress lifecycle', () => {
    beforeEach(() => {
        useMcpStore.setState({ inflightMcpCalls: new Map() });
    });

    it('removes an inflight token when the durable terminal progress event arrives', () => {
        const progress = {
            type: 'mcp_tool_progress' as const,
            progressToken: 'progress-1',
            serverName: 'research',
            toolName: 'search',
            progress: 1,
            total: 2,
            message: 'working',
            runId: 'run-1',
            toolUseId: 'tool-1',
            terminal: false,
        };
        useMcpStore.getState().updateMcpProgress(progress);
        expect(useMcpStore.getState().inflightMcpCalls.has('progress-1')).toBe(true);

        useMcpStore.getState().updateMcpProgress({
            ...progress,
            progress: 0,
            total: 0,
            message: '',
            terminal: true,
        });
        expect(useMcpStore.getState().inflightMcpCalls.has('progress-1')).toBe(false);
    });
});
