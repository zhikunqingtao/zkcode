import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { CurrentWorkbenchView, RunSummary } from '@/hooks/useSimpleWorkbenchData';
import { AgentRunTree } from './AgentRunTree';

const run = (id: string, status: RunSummary['status'], parentRunId: string | null): RunSummary => ({
    id,
    sessionId: 'session-1',
    taskId: `task-${id}`,
    parentRunId,
    status,
    agentType: parentRunId ? 'subagent' : 'query',
    startedAt: '2026-09-09T00:00:00Z',
    finishedAt: status === 'completed' || status === 'failed' ? '2026-09-09T00:01:00Z' : null,
    updatedAt: '2026-09-09T00:01:00Z',
    verificationStatus: 'notRequested',
});

describe('AgentRunTree', () => {
    it('renders canonical lowerCamelCase Run states without normalization', () => {
        const root = run('root', 'completed', null);
        const current: CurrentWorkbenchView = {
            correlationMode: 'EXACT',
            requestMessageId: 'request-1',
            resultMessageId: 'result-1',
            rootTask: null,
            taskTree: [],
            rootRun: root,
            runTree: [
                root,
                run('waiting-child', 'waitingDependencies', root.id),
                run('failed-child', 'failed', root.id),
            ],
            usage: {
                inputTokens: 10,
                outputTokens: 5,
                cacheReadTokens: 0,
                cacheCreateTokens: 0,
                costNanosUsd: 0,
                complete: true,
            },
            eventHighWater: 3,
            activeTools: [],
            request: null,
            result: null,
            structuredSummary: { conclusion: null, completed: [], issues: [], nextSteps: [] },
            delivery: { manifests: [], files: [], totalFiles: 0, primaryArtifactPath: null },
            verification: {
                businessCriteria: [],
                technicalChecks: [],
                evidence: [],
                overallStatus: 'NOT_VERIFIED',
            },
            pendingActionCount: 0,
            pendingActions: [],
            activities: [],
            research: {
                rootTaskId: 'task-root', truncated: false, captures: [], sources: [], findings: [],
                conflicts: [], openQuestions: [], requirementCoverage: [],
            },
            previousAvailableDelivery: null,
            currentFailure: null,
        };

        const { container } = render(<AgentRunTree current={current} />);

        expect(screen.getByText('执行结束')).toBeInTheDocument();
        expect(screen.getByText('等待子任务')).toBeInTheDocument();
        expect(screen.getByText('失败')).toBeInTheDocument();
        expect(container.querySelector('svg.text-emerald-500')).not.toBeNull();
        expect(container.querySelector('svg.animate-spin')).toBeNull();
    });
});
