import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { Message } from '@/types';
import AssistantMessage from './AssistantMessage';
import GroupedToolUseBlock from './GroupedToolUseBlock';

vi.mock('@/hooks/useTtsAvailability', () => ({
    useTtsAvailability: () => false,
}));

describe('AssistantMessage tool presentation', () => {
    it('counts failed and cancelled tools as ended without calling them successful', () => {
        render(<GroupedToolUseBlock toolCalls={{
            a: { toolName: 'Agent', input: {}, status: 'completed', startTime: 1 },
            b: { toolName: 'Agent', input: {}, status: 'error', startTime: 1 },
            c: { toolName: 'Agent', input: {}, status: 'error', startTime: 1, result: { content: 'cancelled', isError: true } },
        }} />);
        expect(screen.getByText(/3\/3 done/)).toBeInTheDocument();
        expect(screen.getByText(/2 failed/)).toBeInTheDocument();
    });
    it('collapses adjacent tool calls into one group', () => {
        const message: Extract<Message, { type: 'assistant' }> = {
            type: 'assistant',
            uuid: 'assistant-tools',
            timestamp: 1,
            stopReason: 'tool_use',
            usage: {
                inputTokens: 0,
                outputTokens: 0,
                cacheReadInputTokens: 0,
                cacheCreationInputTokens: 0,
            },
            content: [1, 2, 3].map(index => ({
                type: 'tool_use' as const,
                toolUseId: `agent-${index}`,
                toolName: 'Agent',
                input: { description: `research-${index}` },
                result: {
                    content: `raw child receipt ${index}`,
                    isError: false,
                },
            })),
        };

        render(<AssistantMessage message={message} />);

        expect(screen.getByText('3 tool calls')).toBeInTheDocument();
        expect(screen.getByText('3/3 done')).toBeInTheDocument();
        expect(screen.queryByText('raw child receipt 1')).not.toBeInTheDocument();
    });
});
