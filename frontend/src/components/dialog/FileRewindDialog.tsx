import { useEffect, useRef, useState } from 'react';
import { Button, Dialog } from '@/components/ui';
import { useSessionStore } from '@/store/sessionStore';
interface Checkpoint { messageId: string; trackedFiles: string[]; timestamp: string }
interface Preview { previewToken: string; expiresInSeconds: number; files: { filePath: string; currentBytes: number | null; restoredBytes: number }[] }
interface Result { success: boolean; restoredFiles: string[]; skippedFiles: string[]; errors: string[] }
export function FileRewindDialog({ sessionId, onClose }: { sessionId: string; onClose: () => void }) {
    const active = useSessionStore(state => state.sessionId);
    const [checkpoints, setCheckpoints] = useState<Checkpoint[]>([]);
    const [checkpoint, setCheckpoint] = useState('');
    const [selected, setSelected] = useState<string[]>([]);
    const [preview, setPreview] = useState<Preview | null>(null);
    const [result, setResult] = useState<Result | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [busy, setBusy] = useState(false);
    const [applying, setApplying] = useState(false);
    const pending = useRef<AbortController | null>(null);
    const current = checkpoints.find(item => item.messageId === checkpoint);
    useEffect(() => {
        const controller = new AbortController(); pending.current = controller;
        setBusy(true);
        void (async () => {
            try {
                const response = await fetch(`/api/sessions/${encodeURIComponent(sessionId)}/history/snapshots`, { headers: { 'X-Session-Id': sessionId }, signal: controller.signal });
                if (!response.ok) throw new Error(`检查点读取失败（${response.status}）`);
                const data = await response.json() as Record<string, Checkpoint[]>;
                const entries = Object.values(data).flat();
                if (!entries.every(item => typeof item.messageId === 'string' && Array.isArray(item.trackedFiles) && item.trackedFiles.every(path => typeof path === 'string'))) throw new Error('检查点响应无效');
                if (!controller.signal.aborted) { setCheckpoints(entries); setCheckpoint(entries.at(-1)?.messageId ?? ''); }
            } catch (failure) { if (!controller.signal.aborted) setError(failure instanceof Error ? failure.message : '读取失败'); }
            finally { if (!controller.signal.aborted) setBusy(false); }
        })();
        return () => pending.current?.abort();
    }, [sessionId]);
    useEffect(() => { if (active !== sessionId) { pending.current?.abort(); onClose(); } }, [active, sessionId, onClose]);
    const submit = async (confirm: boolean) => {
        if (busy || active !== sessionId || (!confirm && !selected.length) || (confirm && !preview)) return;
        if (confirm && !window.confirm(`确认将已选择的 ${preview!.files.length} 个文件恢复为检查点内容？其他执行副作用不会撤销。`)) return;
        const controller = new AbortController(); pending.current = controller;
        setBusy(true); setApplying(confirm); setError(null); setResult(null);
        try {
            const response = await fetch(`/api/sessions/${encodeURIComponent(sessionId)}/history/rewind${confirm ? '' : '/preview'}`, {
                method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Session-Id': sessionId }, signal: controller.signal,
                body: JSON.stringify(confirm ? { previewToken: preview!.previewToken, confirmed: true } : { messageId: checkpoint, filePaths: selected }),
            });
            const data = await response.json() as Preview & Result & { code?: string };
            if (!response.ok) throw new Error(`文件回退${confirm ? '确认' : '预览'}失败（${data.code ?? response.status}）`);
            if (controller.signal.aborted || useSessionStore.getState().sessionId !== sessionId) return;
            if (confirm) {
                if (typeof data.success !== 'boolean' || ![data.restoredFiles, data.skippedFiles, data.errors].every(list => Array.isArray(list) && list.every(value => typeof value === 'string'))) throw new Error('回退响应不完整；请检查文件状态后重新预览。');
                setResult(data); setPreview(null);
            } else {
                if (typeof data.previewToken !== 'string' || !Array.isArray(data.files) || !data.files.length) throw new Error('预览响应无效');
                setPreview(data);
            }
        } catch (failure) {
            if (!controller.signal.aborted) { setError(failure instanceof Error ? failure.message : '请求失败'); if (confirm) setPreview(null); }
        } finally { if (!controller.signal.aborted) { setBusy(false); setApplying(false); } }
    };
    return <Dialog open title="文件检查点回退" onClose={() => { if (!applying) onClose(); }} className="max-w-2xl max-h-[85dvh] overflow-y-auto">
        <div className="p-5 space-y-4 text-sm">
            <p className="text-t2">只恢复所选文件的已记录字节；不会撤销终端、网络、数据库操作或 Git 提交。预览有效期五分钟，文件变化后必须重新预览。</p>
            <label className="block">文件检查点<select aria-label="文件检查点" className="ml-2 max-w-full border border-hairline bg-surfacev2 p-2" value={checkpoint} disabled={busy} onChange={event => { setCheckpoint(event.target.value); setSelected([]); setPreview(null); setResult(null); }}>
                {!checkpoints.length && <option value="">暂无文件检查点</option>}
                {checkpoints.map(item => <option key={item.messageId} value={item.messageId}>{item.timestamp} · {item.trackedFiles.length} 个文件</option>)}
            </select></label>
            <fieldset disabled={busy} className="space-y-2"><legend className="mb-2 text-t2">明确选择需要恢复的文件</legend>
                {[...new Set(current?.trackedFiles ?? [])].map(path => <label key={path} className="flex gap-2 break-all"><input type="checkbox" checked={selected.includes(path)} onChange={event => { setSelected(previous => event.target.checked ? [...previous, path] : previous.filter(item => item !== path)); setPreview(null); setResult(null); }} />{path}</label>)}
            </fieldset>
            {preview && <ul className="space-y-1 bg-sunken2 p-3">{preview.files.map(file => <li key={file.filePath} className="break-all">{file.filePath}：当前 {file.currentBytes ?? '不存在'} → 恢复 {file.restoredBytes} 字节</li>)}</ul>}
            {result && <div role={result.success ? 'status' : 'alert'} className={result.success ? 'text-ok' : 'text-err'}><p>{result.success ? '文件回退完成' : '文件回退未全部完成'}：已恢复 {result.restoredFiles.length} 个文件。</p>{result.restoredFiles.map(path => <p key={`restored-${path}`} className="break-all">已恢复：{path}</p>)}{result.errors.map((message, index) => <p key={index} className="break-all">{message}</p>)}{result.skippedFiles.map(path => <p key={`skipped-${path}`} className="break-all">未处理：{path}</p>)}</div>}
            {error && <p role="alert" className="text-err">{error}</p>}
            <div className="flex gap-2"><Button disabled={!selected.length || busy} onClick={() => void submit(false)}>预览所选文件</Button>{preview && <Button variant="primary" disabled={busy} onClick={() => void submit(true)}>{applying ? '正在确认文件状态…' : '确认回退'}</Button>}</div>
        </div>
    </Dialog>;
}
