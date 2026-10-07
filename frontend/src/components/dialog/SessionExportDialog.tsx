import { useEffect, useRef, useState } from 'react';
import { Button, Dialog } from '@/components/ui';
import { useSessionStore } from '@/store/sessionStore';

export function SessionExportDialog({ sessionId, initialFormat, onClose }: { sessionId: string; initialFormat?: string; onClose: () => void }) {
    const currentSession = useSessionStore(state => state.sessionId);
    const [format, setFormat] = useState(initialFormat === 'markdown' ? 'markdown' : 'json');
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const request = useRef<AbortController | null>(null);
    useEffect(() => () => request.current?.abort(), []);
    useEffect(() => { if (currentSession !== sessionId) onClose(); }, [currentSession, sessionId, onClose]);
    const download = async () => {
        if (request.current || useSessionStore.getState().sessionId !== sessionId) return;
        const controller = new AbortController();
        request.current = controller; setBusy(true); setError(null);
        try {
            const response = await fetch(`/api/sessions/${encodeURIComponent(sessionId)}/export?format=${format}`, {
                method: 'POST', headers: { 'X-Session-Id': sessionId }, signal: controller.signal,
            });
            if (!response.ok) {
                const result = await response.json().catch(() => ({})) as { code?: string };
                throw new Error(result.code === 'EPHEMERAL_OPERATION_UNSUPPORTED' ? '临时会话不能导出。' : `导出失败（${result.code ?? response.status}）`);
            }
            const blob = await response.blob();
            if (controller.signal.aborted || useSessionStore.getState().sessionId !== sessionId) return;
            const url = URL.createObjectURL(blob);
            const anchor = document.createElement('a');
            anchor.href = url;
            // The identity is never used as a path and the server cannot inject a filename.
            anchor.download = `session-${sessionId.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 80)}.${format === 'markdown' ? 'md' : 'json'}`;
            document.body.append(anchor); anchor.click(); anchor.remove();
            setTimeout(() => URL.revokeObjectURL(url), 0);
        } catch (failure) {
            if (!controller.signal.aborted) setError(failure instanceof Error ? failure.message : '导出失败');
        } finally {
            if (request.current === controller) {
                request.current = null;
                if (!controller.signal.aborted) setBusy(false);
            }
        }
    };
    return <Dialog open title="导出当前会话" onClose={onClose} className="max-w-md">
        <div className="p-5 space-y-4">
            <p className="text-sm text-t2">下载包含会话正文与工具内容。请选择需要的格式。</p>
            <label className="block text-sm text-t1">格式
                <select aria-label="导出格式" value={format} disabled={busy} onChange={event => setFormat(event.target.value)} className="mt-1 w-full rounded border border-hairline bg-surfacev2 p-2">
                    <option value="json">JSON</option><option value="markdown">Markdown</option>
                </select>
            </label>
            {error && <p role="alert" className="text-sm text-err">{error}</p>}
            <Button variant="primary" disabled={busy || currentSession !== sessionId} onClick={() => void download()}>{busy ? '准备下载…' : '下载'}</Button>
        </div>
    </Dialog>;
}
