import { useCallback, useEffect, useState } from 'react';
import { useMcpServiceStore } from '@/store/mcpServiceStore';

interface AuthorizationStart { authorizationUrl: string; issuer: string; resource: string; redirectUri: string; scope?: string; expiresIn: number }
interface AuthorizationStatus { state: 'idle' | 'pending' | 'authorized' | 'error'; error?: string }

async function readResponse<T>(response: Response): Promise<T> {
    const value = await response.json();
    if (!response.ok) throw new Error(value.message || 'OAuth 操作失败');
    return value as T;
}

/** Secrets are held only in the current form until its request is sent. */
export function McpServiceOAuth({ name, enabled }: { name: string; enabled: boolean }) {
    const [expanded, setExpanded] = useState(false);
    const [clientId, setClientId] = useState('');
    const [clientSecret, setClientSecret] = useState('');
    const [status, setStatus] = useState<AuthorizationStatus>({ state: 'idle' });
    const [start, setStart] = useState<AuthorizationStart | null>(null);
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [notice, setNotice] = useState<string | null>(null);
    const path = `/api/mcp/services/${encodeURIComponent(name)}/oauth`;
    const fetchServices = useMcpServiceStore(state => state.fetchServices);

    const refresh = useCallback(async () => {
        const next = await readResponse<AuthorizationStatus>(await fetch(path));
        setStatus(next);
        if (next.state !== 'pending') setStart(null);
        if (next.error) setError(next.error);
        return next;
    }, [path]);

    useEffect(() => {
        if (!expanded) return;
        let active = true;
        let timer: ReturnType<typeof setTimeout> | undefined;
        const check = async () => {
            try {
                const next = await refresh();
                if (active && next.state === 'pending') timer = setTimeout(() => void check(), 2000);
                else if (active && next.state === 'authorized') void fetchServices();
            } catch (failure) { if (active) setError(failure instanceof Error ? failure.message : '授权状态读取失败'); }
        };
        void check();
        return () => { active = false; if (timer) clearTimeout(timer); };
    }, [expanded, refresh, fetchServices, start?.authorizationUrl]);

    const authorize = async () => {
        setBusy(true); setError(null); setNotice(null);
        const body = JSON.stringify({ ...(clientId.trim() ? { clientId: clientId.trim() } : {}), ...(clientSecret ? { clientSecret } : {}) });
        setClientSecret('');
        try {
            const result = await readResponse<AuthorizationStart>(await fetch(`${path}/authorize`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body }));
            // Public endpoint URLs must never become script/data links in the UI.
            const url = new URL(result.authorizationUrl);
            if (url.protocol !== 'https:') throw new Error('授权地址必须使用 HTTPS');
            setStart(result); setStatus({ state: 'pending' });
        } catch (failure) { setError(failure instanceof Error ? failure.message : '授权启动失败'); }
        finally { setBusy(false); }
    };

    const logout = async () => {
        setBusy(true); setError(null); setNotice(null);
        try {
            const result = await readResponse<{ loggedOut: boolean; remoteRevoked: boolean }>(await fetch(`${path}/logout`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' }));
            if (!result.loggedOut) throw new Error('未确认退出授权');
            setStart(null); setStatus({ state: 'idle' });
            setNotice(result.remoteRevoked ? '已退出本地授权，并完成远端撤销。' : '已退出本地授权；远端撤销未确认，可在服务提供方账户中检查授权。');
            await fetchServices();
        } catch (failure) { setError(failure instanceof Error ? failure.message : '退出授权失败'); }
        finally { setBusy(false); }
    };

    return <div className="mt-2 text-sm">
        <button type="button" aria-expanded={expanded} className="text-accent" onClick={() => setExpanded(value => !value)}>OAuth 授权设置</button>
        {expanded && <div className="mt-2 space-y-2">
            <p className="text-t2">授权令牌保存在 macOS 钥匙串。现有 Bearer 配置仍可使用；仅在服务要求 OAuth 时配置这里。</p>
            <p role="status">{({ idle: '尚未授权', pending: '等待浏览器授权', authorized: '授权已保存', error: '授权失败' })[status.state]}</p>
            <label className="block">客户端 ID（可选）<input aria-label={`${name} OAuth 客户端 ID`} className="input-field mt-1 w-full" value={clientId} onChange={event => setClientId(event.target.value)} autoComplete="off" /></label>
            <label className="block">客户端密钥（仅预注册应用需要）<input type="password" aria-label={`${name} OAuth 客户端密钥`} className="input-field mt-1 w-full" value={clientSecret} onChange={event => setClientSecret(event.target.value)} autoComplete="off" /></label>
            <p className="text-t2">未填写客户端 ID 时，尝试服务提供方支持的动态注册。</p>
            <div className="flex flex-wrap gap-2">
                <button type="button" className="btn-secondary" disabled={busy || !enabled} onClick={() => void authorize()}>开始授权</button>
                <button type="button" className="btn-secondary" disabled={busy} onClick={() => void logout()}>退出授权</button>
                <button type="button" className="btn-secondary" disabled={busy} onClick={() => void refresh().catch(failure => setError(failure instanceof Error ? failure.message : '授权状态读取失败'))}>检查授权状态</button>
            </div>
            {start && <div className="rounded-lg border border-hairline p-2">
                <p className="break-all">授权方：{start.issuer}</p>
                <p className="break-all">服务：{start.resource}</p>
                {start.scope && <p className="break-all">请求范围：{start.scope}</p>}
                <a className="text-accent underline" href={start.authorizationUrl} target="_blank" rel="noopener noreferrer">在浏览器中审核并授权</a>
                <p className="text-t2">请在 {Math.ceil(start.expiresIn / 60)} 分钟内完成。完成后状态会自动刷新。</p>
            </div>}
            {notice && <p>{notice}</p>}
            {error && <p role="alert" className="text-red-600">{error}</p>}
        </div>}
    </div>;
}
