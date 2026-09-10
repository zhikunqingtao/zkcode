import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { ResearchProjection } from '@/hooks/useSimpleWorkbenchData';
import { ResearchQualitySummary } from './ResearchQualitySummary';

const research = (): ResearchProjection => ({
    rootTaskId: 'task-root',
    truncated: true,
    captures: [],
    sources: [{
        sourceId: 'source-1',
        taskId: 'task-child',
        runId: 'run-child',
        sourceKind: 'webFetch',
        url: 'https://example.com/report',
        title: 'Primary report',
        provider: 'example',
        fetchedAt: '2026-09-09T00:00:00Z',
        httpStatus: 200,
        truncated: false,
    }],
    findings: [{
        findingId: 'finding-1',
        sourceId: 'source-1',
        findingKind: 'quote',
        excerpt: 'A supported finding',
        rank: 1,
    }],
    conflicts: [{ status: 'open', summary: '两个来源的数字不一致', resolution: null }],
    openQuestions: [{ status: 'open', question: '发布日期是否已经确认？', resolution: null }],
    requirementCoverage: [],
});

describe('ResearchQualitySummary', () => {
    it('shows provenance, truncation and unresolved research quality signals', () => {
        render(<ResearchQualitySummary research={research()} />);

        expect(screen.getByRole('link', { name: /Primary report/ })).toHaveAttribute(
            'href', 'https://example.com/report',
        );
        expect(screen.getByText('1 个来源 · 1 条摘录 · 2 项未解决')).toBeInTheDocument();
        expect(screen.getByText(/展示内容已达到安全上限/)).toBeInTheDocument();
        expect(screen.getByText('两个来源的数字不一致')).toBeInTheDocument();
        expect(screen.getByText('发布日期是否已经确认？')).toBeInTheDocument();
    });

    it('renders nothing for an empty projection', () => {
        const empty = research();
        empty.truncated = false;
        empty.sources = [];
        empty.findings = [];
        empty.conflicts = [];
        empty.openQuestions = [];

        const { container } = render(<ResearchQualitySummary research={empty} />);
        expect(container).toBeEmptyDOMElement();
    });
});
