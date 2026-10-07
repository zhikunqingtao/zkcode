import { useEffect, useRef, useState } from 'react';
import { FileText } from 'lucide-react';
import { useResponsive } from '@/hooks/useResponsive';

interface Checkpoint { messageId: string; trackedFiles: string[]; timestamp: string }
interface DiffStats { filesAdded: number; filesModified: number; filesDeleted: number; changedFiles: string[] }
const record = (value: unknown): value is Record<string, unknown> => !!value && typeof value === 'object' && !Array.isArray(value);
function checkpoints(value: unknown): Checkpoint[] {
    if (!record(value)) throw new Error('检查点列表格式无效');
    const result: Checkpoint[] = [];
    for (const [id, entries] of Object.entries(value)) {
        if (!Array.isArray(entries)) throw new Error('检查点列表格式无效');
        for (const entry of entries) {
            if (!record(entry) || entry.messageId !== id || typeof entry.timestamp !== 'string'
                || !Array.isArray(entry.trackedFiles) || !entry.trackedFiles.every(path => typeof path === 'string')) throw new Error('检查点列表格式无效');
            result.push(entry as unknown as Checkpoint);
        }
    }
    return result.sort((a, b) => a.timestamp.localeCompare(b.timestamp) || a.messageId.localeCompare(b.messageId));
}
function stats(value: unknown): DiffStats {
    if (!record(value) || !['filesAdded', 'filesModified', 'filesDeleted'].every(key => Number.isSafeInteger(value[key]) && Number(value[key]) >= 0)
        || !Array.isArray(value.changedFiles) || !value.changedFiles.every(path => typeof path === 'string')) throw new Error('变更统计格式无效');
    return value as unknown as DiffStats;
}

/** A comparison of two actual checkpoints, distinct from the live Git working tree. */
export function FileChangesDashboard({ sessionId }: { sessionId: string }) {
    const [loadedFor, setLoadedFor] = useState<string | null>(null);
    const [turns, setTurns] = useState<Checkpoint[]>([]);
    const [from, setFrom] = useState(''); const [to, setTo] = useState('');
    const [selected, setSelected] = useState<string | null>(null);
    const [result, setResult] = useState<DiffStats | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [busy, setBusy] = useState(false);
    const [tab, setTab] = useState<'files' | 'diff'>('files');
    const active = useRef<AbortController | null>(null);
    const epoch = useRef(0);
    const { isMobile } = useResponsive();
    useEffect(() => {
        const version = ++epoch.current; const controller = new AbortController();
        active.current?.abort(); active.current = controller;
        setLoadedFor(null); setTurns([]); setResult(null); setError(null); setSelected(null); setBusy(false);
        void fetch(`/api/sessions/${encodeURIComponent(sessionId)}/history/snapshots`, { headers: { 'X-Session-Id': sessionId }, signal: controller.signal })
            .then(async response => { if (!response.ok) throw new Error(`读取检查点失败（HTTP ${response.status}）`); return checkpoints(await response.json()); })
            .then(value => { if (!controller.signal.aborted && version === epoch.current) { setTurns(value); setFrom(value[0]?.messageId ?? ''); setTo(value.at(-1)?.messageId ?? ''); } })
            .catch(failure => { if (!controller.signal.aborted && version === epoch.current) setError(failure instanceof Error ? failure.message : '读取检查点失败'); })
            .finally(() => { if (!controller.signal.aborted && version === epoch.current) setLoadedFor(sessionId); });
        return () => { controller.abort(); active.current?.abort(); };
    }, [sessionId]);
    const resetComparison = () => { epoch.current++; active.current?.abort(); setResult(null); setError(null); setBusy(false); };
    const compare = async () => {
        if (!from || !to || from === to || busy) return;
        const version = ++epoch.current; active.current?.abort(); const controller = new AbortController(); active.current = controller;
        setBusy(true); setResult(null); setError(null);
        try {
            const query = new URLSearchParams({ fromMessageId: from, toMessageId: to });
            const response = await fetch(`/api/sessions/${encodeURIComponent(sessionId)}/history/diff?${query}`, { headers: { 'X-Session-Id': sessionId }, signal: controller.signal });
            if (!response.ok) throw new Error(`读取变更统计失败（HTTP ${response.status}）`);
            const value = stats(await response.json());
            if (!controller.signal.aborted && version === epoch.current) setResult(value);
        } catch (failure) { if (!controller.signal.aborted && version === epoch.current) setError(failure instanceof Error ? failure.message : '读取变更统计失败'); }
        finally { if (version === epoch.current) setBusy(false); }
    };
    if (loadedFor !== sessionId) return <p role="status" className="p-4 text-sm text-t2">正在读取文件检查点…</p>;
    const files = [...new Set(turns.flatMap(turn => turn.trackedFiles))];
    const fileList = <div className={isMobile ? '' : 'w-64 shrink-0 overflow-auto border-r border-hairline'}>
        <h3 className="p-3 text-base font-semibold">检查点文件（{files.length}）</h3>
        {files.map(path => <button type="button" key={path} className={`panel-control flex w-full items-center gap-2 px-3 py-2 text-left text-sm ${selected === path ? 'bg-accent2-soft text-accent2-ink' : 'text-t2'}`}
            onClick={() => { resetComparison(); setSelected(path); const candidates = turns.filter(turn => turn.trackedFiles.includes(path)); setFrom(candidates[0]?.messageId ?? ''); setTo(candidates.at(-1)?.messageId ?? ''); setTab('diff'); }}>
            <FileText size={14} /><span className="truncate" title={path}>{path.split('/').pop()}</span></button>)}
    </div>;
    const detail = <div className="min-w-0 flex-1 space-y-3 overflow-auto p-4 text-sm text-t2">
        <p>比较两个已保存检查点的全部文件，不代表当前工作区或 Git 变更。</p>
        {selected && <p className="break-all">已按文件筛选初始检查点：{selected}</p>}
        {turns.length < 2 ? <p>至少需要两个文件检查点才能比较。</p> : <>
            <label className="block">起点检查点<select aria-label="起点检查点" value={from} onChange={event => { resetComparison(); setFrom(event.target.value); }} className="ml-2 rounded border border-hairline bg-surfacev2 p-2 text-t1">{turns.map(turn => <option key={turn.messageId} value={turn.messageId}>{turn.timestamp} · {turn.messageId}</option>)}</select></label>
            <label className="block">终点检查点<select aria-label="终点检查点" value={to} onChange={event => { resetComparison(); setTo(event.target.value); }} className="ml-2 rounded border border-hairline bg-surfacev2 p-2 text-t1">{turns.map(turn => <option key={turn.messageId} value={turn.messageId}>{turn.timestamp} · {turn.messageId}</option>)}</select></label>
            <button type="button" onClick={() => void compare()} disabled={busy || !from || !to || from === to} className="panel-control rounded border border-hairline p-2 disabled:opacity-50">{busy ? '正在比较…' : '比较检查点'}</button>
        </>}
        {error && <p role="alert" className="text-err">{error}</p>}
        {result && <div><p>新增 {result.filesAdded} · 修改 {result.filesModified} · 删除 {result.filesDeleted}</p><ul>{result.changedFiles.map(path => <li key={path} className="break-all font-mono">{path}</li>)}</ul></div>}
    </div>;
    return <div className={`flex h-full ${isMobile ? 'flex-col' : ''}`}>
        {isMobile && <div className="flex border-b border-hairline"><button className="panel-control flex-1 p-2" onClick={() => setTab('files')}>文件列表</button><button className="panel-control flex-1 p-2" onClick={() => setTab('diff')}>检查点比较</button></div>}
        {(!isMobile || tab === 'files') && fileList}{(!isMobile || tab === 'diff') && detail}
    </div>;
}
