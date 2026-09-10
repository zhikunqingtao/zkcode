import { AlertTriangle, ExternalLink, Search } from 'lucide-react';
import type { ResearchProjection } from '@/hooks/useSimpleWorkbenchData';

export function ResearchQualitySummary({ research }: { research: ResearchProjection | null | undefined }) {
    if (!research || (research.sources.length === 0
        && research.conflicts.length === 0
        && research.openQuestions.length === 0
        && research.requirementCoverage.length === 0)) return null;

    const unresolved = research.conflicts.filter(item => item.status !== 'resolved').length
        + research.openQuestions.filter(item => item.status !== 'resolved').length;

    return <section className="rounded-2xl border border-[var(--border)] bg-[var(--bg-secondary)] p-5">
        <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex items-center gap-2">
                <Search className="h-4 w-4 text-blue-400" />
                <h2 className="text-sm font-semibold text-[var(--text-primary)]">调研依据</h2>
            </div>
            <p className="text-xs text-[var(--text-muted)]">
                {research.sources.length} 个来源 · {research.findings.length} 条摘录
                {unresolved > 0 ? ` · ${unresolved} 项未解决` : ''}
            </p>
        </div>
        {research.truncated && <p className="mt-3 rounded-lg border border-amber-500/25 bg-amber-500/5 px-3 py-2 text-xs text-amber-400">
            展示内容已达到安全上限，请通过任务诊断读取剩余记录。
        </p>}
        {research.sources.length > 0 && <ul className="mt-3 grid gap-2 md:grid-cols-2">
            {research.sources.slice(0, 8).map(source => <li key={source.sourceId} className="min-w-0 rounded-xl border border-[var(--border)] bg-[var(--bg-primary)] p-3">
                <a href={source.url} target="_blank" rel="noreferrer" className="flex items-start gap-2 text-sm font-medium text-blue-400 hover:text-blue-300">
                    <span className="min-w-0 flex-1 truncate">{source.title || source.url}</span><ExternalLink className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                </a>
                <p className="mt-1 truncate text-xs text-[var(--text-muted)]">{source.provider || source.sourceKind} · {source.fetchedAt}</p>
            </li>)}
        </ul>}
        {unresolved > 0 && <div className="mt-3 rounded-xl border border-amber-500/25 bg-amber-500/5 p-3">
            <p className="flex items-center gap-2 text-xs font-medium text-amber-400"><AlertTriangle className="h-3.5 w-3.5" />仍需核实</p>
            <ul className="mt-2 space-y-1 text-xs text-[var(--text-secondary)]">
                {research.conflicts.filter(item => item.status !== 'resolved').slice(0, 3).map((item, index) => <li key={`conflict-${index}`}>{item.summary}</li>)}
                {research.openQuestions.filter(item => item.status !== 'resolved').slice(0, 3).map((item, index) => <li key={`question-${index}`}>{item.question}</li>)}
            </ul>
        </div>}
    </section>;
}
