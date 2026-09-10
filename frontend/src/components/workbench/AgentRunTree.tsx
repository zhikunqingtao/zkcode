import { AlertTriangle, Bot, CheckCircle2, CircleDashed, Loader2 } from 'lucide-react';
import type { CurrentWorkbenchView, RunSummary } from '@/hooks/useSimpleWorkbenchData';

const TERMINAL = new Set<RunSummary['status']>(['completed', 'failed', 'cancelled', 'interrupted']);

const STATUS_LABELS: Record<RunSummary['status'], string> = {
    queued: '排队中',
    running: '运行中',
    waitingDependencies: '等待子任务',
    waitingInteraction: '等待用户',
    cancelling: '取消中',
    completed: '执行结束',
    failed: '失败',
    cancelled: '已取消',
    interrupted: '已中断',
};

function statusLabel(run: RunSummary): string {
    return STATUS_LABELS[run.status];
}

function StatusIcon({ run }: { run: RunSummary }) {
    if (run.cleanupStatus?.toLowerCase() === 'unconfirmed') return <AlertTriangle className="h-4 w-4 text-amber-500" />;
    if (run.status === 'completed') return <CheckCircle2 className="h-4 w-4 text-emerald-500" />;
    if (TERMINAL.has(run.status)) return <AlertTriangle className="h-4 w-4 text-red-500" />;
    if (run.status === 'running') return <Loader2 className="h-4 w-4 animate-spin text-blue-500" />;
    return <CircleDashed className="h-4 w-4 text-amber-500" />;
}

export function AgentRunTree({ current }: { current: CurrentWorkbenchView | null }) {
    if (!current?.rootRun) return null;
    const runs = current.runTree.length > 0 ? current.runTree : [current.rootRun];
    const cost = current.usage.costNanosUsd / 1_000_000_000;
    return (
        <section className="rounded-2xl border border-[var(--border)] bg-[var(--bg-secondary)] p-5">
            <div className="flex flex-wrap items-center justify-between gap-2">
                <div><p className="text-xs font-medium uppercase tracking-wide text-blue-500">Agent 执行树</p><p className="mt-1 text-sm text-[var(--text-muted)]">{runs.length} 次执行 · {current.usage.inputTokens + current.usage.outputTokens} tokens</p></div>
                <span className="text-xs text-[var(--text-muted)]">${cost.toFixed(4)}{current.usage.complete ? '' : ' · 用量不完整'}</span>
            </div>
            <div className="mt-4 space-y-2">
                {runs.map((run) => {
                    const child = run.parentRunId != null;
                    const cleanupUnconfirmed = run.cleanupStatus?.toLowerCase() === 'unconfirmed';
                    return <div key={run.id} className={`flex items-center gap-3 rounded-xl border border-[var(--border)] bg-[var(--bg-primary)] px-3 py-2.5 ${child ? 'ml-6' : ''}`}>
                        <StatusIcon run={run} />
                        <Bot className="h-4 w-4 text-[var(--text-muted)]" />
                        <div className="min-w-0 flex-1"><p className="truncate text-sm font-medium text-[var(--text-primary)]">{child ? '子 Agent' : '主 Agent'} · {run.agentType || 'agent'}</p><p className="truncate text-xs text-[var(--text-muted)]">{run.taskId || run.id}</p></div>
                        <div className="text-right"><p className="text-xs font-medium text-[var(--text-secondary)]">{statusLabel(run)}</p>{cleanupUnconfirmed && <p className="text-[11px] text-amber-500">停止未确认</p>}</div>
                    </div>;
                })}
            </div>
        </section>
    );
}
