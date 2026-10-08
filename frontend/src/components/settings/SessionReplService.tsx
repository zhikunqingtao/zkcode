import { useEffect, useRef, useState } from 'react';
import { Button, Dialog } from '@/components/ui';
import { useSessionStore } from '@/store/sessionStore';

interface ReplServiceStatus {
    sessionId: string;
    state: 'absent' | 'starting' | 'running' | 'stopping' | 'stopped' | 'cleanupUnconfirmed';
    cleanupStatus: 'notRequired' | 'pending' | 'confirmed' | 'unconfirmed';
    idleTimeoutSeconds: number;
    maxLifetimeSeconds: number;
    lastActivityAt?: string;
}
function validate(value: unknown, id: string): ReplServiceStatus {
    const item = value as ReplServiceStatus;
    if (!item || item.sessionId !== id || !['absent', 'starting', 'running', 'stopping', 'stopped', 'cleanupUnconfirmed'].includes(item.state)
        || !['notRequired', 'pending', 'confirmed', 'unconfirmed'].includes(item.cleanupStatus)
        || !Number.isFinite(item.idleTimeoutSeconds) || !Number.isFinite(item.maxLifetimeSeconds)) throw new Error('REPL 服务状态响应无效');
    return item;
}
function label(value: ReplServiceStatus): string {
    if (value.state === 'absent' && ['notRequired', 'confirmed'].includes(value.cleanupStatus)) return '未启动';
    if (value.state === 'stopped' && value.cleanupStatus === 'confirmed') return '已停止';
    if (value.state === 'starting') return '启动中';
    if (value.state === 'running' && value.cleanupStatus !== 'unconfirmed') return '运行中';
    if (value.state === 'stopping') return '正在停止，等待清理完成';
    return '清理未确认，请检查后重试';
}
export function SessionReplService() {
    const sessionId = useSessionStore(state => state.sessionId);
    return sessionId ? <BoundSessionReplService key={sessionId} sessionId={sessionId} /> : null;
}
function BoundSessionReplService({ sessionId }: { sessionId: string }) {
    const [value, setValue] = useState<ReplServiceStatus>();
    const [error, setError] = useState<string>();
    const [actionError, setActionError] = useState<string>();
    const [confirming, setConfirming] = useState(false);
    const [saving, setSaving] = useState(false);
    const [refresh, setRefresh] = useState(0);
    const generation = useRef(0);
    const request = useRef<AbortController | null>(null);
    const url = `/api/sessions/${encodeURIComponent(sessionId)}/repl-service`;
    useEffect(() => {
        const current = ++generation.current;
        let timer: ReturnType<typeof setTimeout> | undefined;
        const load = async () => {
            if (generation.current !== current) return;
            const controller = new AbortController();
            request.current = controller;
            let delay = 10_000;
            try {
                const response = await fetch(url, { headers: { 'X-Session-Id': sessionId }, signal: controller.signal });
                if (!response.ok) throw new Error(`读取 REPL 服务失败（HTTP ${response.status}）`);
                const next = validate(await response.json(), sessionId);
                if (generation.current !== current) return;
                setValue(next); setError(undefined);
                delay = ['starting', 'stopping'].includes(next.state) ? 1000 : 5000;
            } catch (cause) {
                if (generation.current !== current || controller.signal.aborted) return;
                setError(cause instanceof Error ? cause.message : '读取 REPL 服务失败');
            }
            if (generation.current === current) timer = setTimeout(() => { void load(); }, delay);
        };
        void load();
        return () => { generation.current = current + 1; request.current?.abort(); clearTimeout(timer); };
    }, [sessionId, url, refresh]);
    const stop = async () => {
        if (saving) return;
        setConfirming(false); setSaving(true); setActionError(undefined);
        const current = ++generation.current;
        request.current?.abort();
        const controller = new AbortController(); request.current = controller;
        try {
            const response = await fetch(url, { method: 'DELETE', headers: { 'X-Session-Id': sessionId }, signal: controller.signal });
            if (!response.ok) throw new Error(`停止 REPL 服务失败（HTTP ${response.status}）`);
            const next = validate(await response.json(), sessionId);
            if (generation.current === current) setValue(next);
        } catch (cause) {
            if (generation.current === current && !controller.signal.aborted) setActionError(cause instanceof Error ? cause.message : '停止 REPL 服务失败');
        } finally {
            if (generation.current === current) { setSaving(false); setRefresh(count => count + 1); }
        }
    };
    const canStop = !saving && (value?.state === 'starting' || value?.state === 'running' || value?.state === 'cleanupUnconfirmed');
    return <section className="space-y-2 border-t border-hairline pt-3" aria-label="当前会话 REPL 服务">
        <div className="flex flex-wrap items-center justify-between gap-2">
            <p>当前会话 REPL 服务：<span role="status">{saving ? '正在请求停止' : value ? label(value) : '读取中'}</span></p>
            <div className="flex gap-2"><Button variant="ghost" disabled={saving} onClick={() => setRefresh(count => count + 1)}>刷新状态</Button>{canStop && <Button variant="danger" onClick={() => setConfirming(true)}>停止 REPL 服务</Button>}</div>
        </div>
        <p className="text-xs text-t3">普通会话的变量可跨轮次保留；停止会清除解释器状态，已写入的文件保留。{value ? ` 空闲 ${value.idleTimeoutSeconds} 秒或运行 ${value.maxLifetimeSeconds} 秒后自动回收。` : ''}</p>
        {error && <p role="alert" className="text-err">{error}</p>}
        {actionError && <p role="alert" className="text-err">{actionError}</p>}
        <Dialog open={confirming} onClose={() => setConfirming(false)} title="停止当前会话 REPL 服务">
            <p className="px-5 text-sm text-t2">正在执行的 REPL 调用会停止，变量和解释器状态会清除。已写入的文件保留。</p>
            <div className="flex justify-end gap-2 p-5"><Button variant="ghost" onClick={() => setConfirming(false)}>取消</Button><Button variant="danger" onClick={() => { void stop(); }}>确认停止 REPL</Button></div>
        </Dialog>
    </section>;
}
