import { useEffect, useRef, useState } from 'react';
import { Button, Dialog } from '@/components/ui';
import { usePageExitGuard } from '@/hooks/usePageExitGuard';
import { useSessionStore } from '@/store/sessionStore';

interface HookDocument { content: string; revision: string; path: string; hookCount: number | null; validationError: string | null; events: string[] }
function ExitGuard() { usePageExitGuard(); return null; }
export function HooksEditor({ sessionId, onClose }: { sessionId: string; onClose: () => void }) {
    const current = useSessionStore(state => state.sessionId);
    const [document, setDocument] = useState<HookDocument | null>(null);
    const [text, setText] = useState('');
    const [error, setError] = useState<string | null>(null);
    const [busy, setBusy] = useState(false);
    const [saved, setSaved] = useState(false);
    const pending = useRef<AbortController | null>(null);
    const dirty = document !== null && text !== document.content;
    const close = () => { if (!dirty || window.confirm('有未保存的 Hook 修改，确定离开吗？')) onClose(); };
    const load = async (controller: AbortController) => {
        setBusy(true); setError(null); setSaved(false);
        try {
            const response = await fetch(`/api/sessions/${encodeURIComponent(sessionId)}/hooks`, { headers: { 'X-Session-Id': sessionId }, signal: controller.signal });
            if (!response.ok) throw new Error(`加载失败（${response.status}）`);
            const result = await response.json() as HookDocument;
            if (typeof result.content !== 'string' || typeof result.revision !== 'string') throw new Error('Hook 配置响应无效');
            if (!controller.signal.aborted && useSessionStore.getState().sessionId === sessionId) { setDocument(result); setText(result.content); }
        } catch (failure) { if (!controller.signal.aborted) setError(failure instanceof Error ? failure.message : '加载失败'); }
        finally { if (!controller.signal.aborted) setBusy(false); }
    };
    useEffect(() => {
        const controller = new AbortController(); pending.current = controller;
        void load(controller);
        return () => pending.current?.abort();
        // The dialog is keyed by session; loading never depends on its draft.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [sessionId]);
    useEffect(() => { if (current !== sessionId) { pending.current?.abort(); onClose(); } }, [current, sessionId, onClose]);
    const save = async () => {
        if (!document || busy || current !== sessionId || !window.confirm('保存仅更新配置，不授予执行权限。后续命令或网络 Hook 仍需按当前权限模式获得授权。确认保存此配置？')) return;
        const controller = new AbortController(); pending.current = controller;
        setBusy(true); setError(null); setSaved(false);
        try {
            const response = await fetch(`/api/sessions/${encodeURIComponent(sessionId)}/hooks`, {
                method: 'PUT', headers: { 'Content-Type': 'application/json', 'X-Session-Id': sessionId }, signal: controller.signal,
                body: JSON.stringify({ revision: document.revision, content: text, confirmed: true }),
            });
            const result = await response.json() as HookDocument & { code?: string };
            if (!response.ok) throw new Error(response.status === 409 ? '配置已变化或当前任务仍在执行；草稿已保留，请重新加载后核对。' : `保存失败（${result.code ?? response.status}）；未确认保存。`);
            if (typeof result.content !== 'string' || typeof result.revision !== 'string') throw new Error('保存响应无效，请重新加载确认。');
            if (!controller.signal.aborted && useSessionStore.getState().sessionId === sessionId) { setDocument(result); setText(result.content); setSaved(true); }
        } catch (failure) { if (!controller.signal.aborted) setError(failure instanceof Error ? failure.message : '保存失败'); }
        finally { if (!controller.signal.aborted) setBusy(false); }
    };
    return <Dialog open onClose={close} title="项目 Hooks" className="max-w-3xl max-h-[85dvh] overflow-y-auto">
        {dirty && <ExitGuard />}
        <div className="p-5 space-y-3 text-sm">
            <p className="text-t2">编辑当前项目 .zk/hooks.toml。保存只修改配置，不授予执行权限；后续运行仍按会话权限模式检查。正在进行的任务结束后才能保存。</p>
            {document?.validationError && <p role="status" className="text-warn">现有配置无效；运行时保留最近有效配置。请修复后显式保存。</p>}
            <textarea aria-label="Hook TOML 配置" value={text} onChange={event => { setText(event.target.value); setSaved(false); }} disabled={!document || busy} spellCheck={false} className="h-80 w-full rounded border border-hairline bg-sunken2 p-3 font-mono text-t1" />
            {error && <p role="alert" className="text-err">{error}</p>}
            {saved && <p role="status" className="text-ok">配置已保存；未执行 Hook。</p>}
            <div className="flex gap-2">
                <Button disabled={busy} onClick={() => { if (!dirty || window.confirm('丢弃当前草稿并重新加载？')) { pending.current?.abort(); const controller = new AbortController(); pending.current = controller; void load(controller); } }}>重新加载</Button>
                <Button variant="primary" disabled={!dirty || busy} onClick={() => void save()}>保存配置</Button>
            </div>
        </div>
    </Dialog>;
}
