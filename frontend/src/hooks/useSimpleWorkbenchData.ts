import { useEffect, useRef, useState } from 'react';
import type { ActivityData } from '@/types/apos';
import type {
    RuntimeCleanupStatus,
    RuntimeRunStatus,
    RuntimeTaskStatus,
    RuntimeVerificationStatus,
} from '@/types';

export interface SessionDetail {
    sessionId: string;
    model: string;
    workingDir: string;
    title: string | null;
    status: string;
    summary: string | null;
    createdAt: string;
    updatedAt: string;
}

export interface RunSummary {
    id: string;
    sessionId: string;
    taskId?: string;
    parentRunId?: string | null;
    status: RuntimeRunStatus;
    agentType?: string;
    startedAt: string | null;
    finishedAt?: string | null;
    updatedAt: string;
    verificationStatus: RuntimeVerificationStatus;
    cleanupStatus?: string;
    inputTokens?: number;
    outputTokens?: number;
    costNanosUsd?: number;
    usageComplete?: boolean;
    errorSummary?: string | null;
}

export interface SubtreeUsage {
    inputTokens: number;
    outputTokens: number;
    cacheReadTokens: number;
    cacheCreateTokens: number;
    costNanosUsd: number;
    complete: boolean;
}

export interface WorkbenchTaskSummary {
    id: string;
    sessionId: string;
    parentTaskId: string | null;
    rootTaskId: string;
    currentRunId: string | null;
    description: string;
    taskType: string;
    status: RuntimeTaskStatus;
    reason: string | null;
    cleanupStatus: RuntimeCleanupStatus;
    verificationStatus: RuntimeVerificationStatus;
    usageComplete: boolean;
    createdAt: string;
    updatedAt: string;
    terminalAt: string | null;
}

export interface WorkbenchActiveTool {
    invocationId: string;
    taskId: string;
    runId: string;
    toolUseId: string;
    toolName: string;
    status: 'preparing' | 'queued' | 'running';
    input: Record<string, unknown> | null;
    sideEffectClass: string;
    cleanupStatus: RuntimeCleanupStatus;
    startedAt: string | null;
    createdAt: string;
}

export interface ArtifactEntrySummary {
    id: string;
    filePath: string;
    operation: 'created' | 'modified' | 'deleted';
    state: string;
    fileSize: number | null;
    verified: boolean;
    mismatchDetail: string | null;
}

export interface ArtifactManifestSummary {
    id: string;
    runId: string;
    sessionId: string;
    workspaceRoot: string;
    status: string;
    createdAt: string;
    updatedAt: string;
    totalFiles: number;
    verifiedFiles: number;
    failedFiles: number;
    entries: ArtifactEntrySummary[];
}

export interface WorkbenchMessage {
    messageId: string;
    text: string;
    timestamp: string;
}

export interface StructuredSummary {
    conclusion: string | null;
    completed: string[];
    issues: string[];
    nextSteps: string[];
}

export type CriterionStatus = 'PASSED' | 'FAILED' | 'PARTIAL' | 'STALE' | 'NOT_VERIFIED';
export interface WorkbenchCriterion {
    id: string | null;
    type: 'business' | 'technical';
    text: string;
    status: CriterionStatus;
    detail: string | null;
    evidenceBundleId: string | null;
}

export interface DeliveryView {
    manifests: ArtifactManifestSummary[];
    files: DeliveryFileView[];
    totalFiles: number;
    primaryArtifactPath: string | null;
}

export interface DeliveryFileView extends ArtifactEntrySummary {
    manifestId: string;
    workspaceRoot: string;
    relativePath: string;
    primary: boolean;
}

export interface WorkbenchPendingAction {
    interactionId: string;
    runId: string;
    interactionType: 'permission' | 'elicitation' | 'plan_approval';
    prompt: Record<string, unknown>;
    createdAt: string;
}

export interface WorkbenchActivity extends ActivityData {
    sessionId: string;
    runId: string;
}

export interface ResearchSourceSummary {
    sourceId: string;
    taskId: string;
    runId: string;
    sourceKind: string;
    url: string;
    title: string | null;
    provider: string | null;
    fetchedAt: string;
    httpStatus: number | null;
    truncated: boolean;
}

export interface ResearchFindingSummary {
    findingId: string;
    sourceId: string;
    findingKind: string;
    excerpt: string;
    rank: number | null;
}

export interface ResearchIssueSummary {
    status: string;
    summary?: string;
    question?: string;
    resolution: string | null;
}

export interface ResearchRequirementCoverageSummary {
    coverageId: string;
    requirementKey: string;
    requirementText: string;
    status: string;
    supportingFindingId: string | null;
    notes: string | null;
}

export interface ResearchProjection {
    rootTaskId: string;
    truncated: boolean;
    captures: unknown[];
    sources: ResearchSourceSummary[];
    findings: ResearchFindingSummary[];
    conflicts: ResearchIssueSummary[];
    openQuestions: ResearchIssueSummary[];
    requirementCoverage: ResearchRequirementCoverageSummary[];
}

export interface CurrentWorkbenchView {
    /** EXACT is the only bound state; UNBOUND is an integrity signal, never a guessed fallback. */
    correlationMode: 'EXACT' | 'EMPTY' | 'UNBOUND';
    requestMessageId: string | null;
    resultMessageId: string | null;
    rootTask: WorkbenchTaskSummary | null;
    taskTree: WorkbenchTaskSummary[];
    rootRun: RunSummary | null;
    runTree: RunSummary[];
    usage: SubtreeUsage;
    eventHighWater: number;
    activeTools: WorkbenchActiveTool[];
    request: WorkbenchMessage | null;
    result: WorkbenchMessage | null;
    structuredSummary: StructuredSummary;
    delivery: DeliveryView;
    verification: {
        businessCriteria: WorkbenchCriterion[];
        technicalChecks: WorkbenchCriterion[];
        evidence: unknown[];
        overallStatus: CriterionStatus;
    };
    pendingActionCount: number;
    pendingActions: WorkbenchPendingAction[];
    activities: WorkbenchActivity[];
    research: ResearchProjection;
    previousAvailableDelivery: {
        rootRunId: string;
        finishedAt: string | null;
        result: WorkbenchMessage | null;
        delivery: DeliveryView;
    } | null;
    currentFailure: { status: string; reason: string } | null;
}

interface SectionState<T> { loading: boolean; error: string | null; data: T; }
export interface SimpleWorkbenchData {
    session: SectionState<SessionDetail | null>;
    current: SectionState<CurrentWorkbenchView | null>;
}

const emptyState = (): SimpleWorkbenchData => ({
    session: { loading: false, error: null, data: null },
    current: { loading: false, error: null, data: null },
});

async function readJson<T>(url: string, sessionId: string, signal: AbortSignal): Promise<T> {
    const response = await fetch(url, { headers: { 'X-Session-Id': sessionId }, signal });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json() as Promise<T>;
}

export function useSimpleWorkbenchData(sessionId: string | null): SimpleWorkbenchData {
    const [state, setState] = useState<SimpleWorkbenchData>(emptyState);
    const generationRef = useRef(0);

    useEffect(() => {
        const generation = ++generationRef.current;
        const controller = new AbortController();
        if (!sessionId) {
            setState(emptyState());
            return () => controller.abort();
        }
        const isCurrent = () => generationRef.current === generation && !controller.signal.aborted;
        setState({
            session: { loading: true, error: null, data: null },
            current: { loading: true, error: null, data: null },
        });

        void readJson<SessionDetail>(`/api/sessions/${encodeURIComponent(sessionId)}`, sessionId, controller.signal)
            .then(data => isCurrent() && setState(previous => ({ ...previous, session: { loading: false, error: null, data } })))
            .catch(error => isCurrent() && setState(previous => ({ ...previous, session: { loading: false, error: error instanceof Error ? error.message : String(error), data: null } })));

        const fetchCurrent = () => readJson<CurrentWorkbenchView>(
            `/api/sessions/${encodeURIComponent(sessionId)}/workbench/current`, sessionId, controller.signal,
        ).then(data => {
            if (!isCurrent()) return;
            setState(previous => ({ ...previous, current: { loading: false, error: null, data } }));
        }).catch(error => {
            if (!isCurrent()) return;
            setState(previous => ({ ...previous, current: { loading: false, error: error instanceof Error ? error.message : String(error), data: null } }));
        });
        void fetchCurrent();
        const poll = window.setInterval(() => { void fetchCurrent(); }, 5000);
        return () => { controller.abort(); window.clearInterval(poll); };
    }, [sessionId]);

    return state;
}
