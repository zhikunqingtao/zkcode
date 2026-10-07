import { useEffect } from 'react';
import { useMcpServiceStore } from '@/store/mcpServiceStore';
import { McpServiceOAuth } from './McpServiceOAuth';

const labels: Record<string, string> = {
    connected: '已连接', disconnected: '未连接', disabled: '已关闭',
    failed: '连接失败', pending: '连接中', degraded: '连接异常', needs_auth: '需要授权',
};

export function McpServicesPanel() {
    const { services, loading, changing, error, fetchServices, setEnabled } = useMcpServiceStore();
    useEffect(() => { void fetchServices(); }, [fetchServices]);
    return <section className="space-y-4" aria-label="MCP 服务">
        <div className="flex items-center justify-between gap-3">
            <p className="text-sm text-t2">服务开关影响所有项目。关闭服务会断开本地连接并阻止新调用，保留各工具的启用设置。</p>
            <button type="button" className="btn-secondary shrink-0" disabled={loading || changing !== null} onClick={() => void fetchServices()}>刷新</button>
        </div>
        {error && <p role="alert" className="text-sm text-red-600">{error}</p>}
        {loading && <p role="status">正在读取服务状态…</p>}
        {!loading && services.length === 0 && !error && <p className="text-sm text-t2">尚未配置 MCP 服务。可在 MCP 配置文件或工具目录中添加服务。</p>}
        <ul className="space-y-2">{services.map(service => <li key={service.name} className="flex items-center justify-between gap-3 rounded-lg border border-hairline p-3">
            <div className="min-w-0">
                <h3 className="break-all font-medium">{service.name}</h3>
                <p className="text-sm text-t2">{service.transport} · {labels[service.status] || service.status} · {service.toolCount} 个可调用工具</p>
                {['HTTP', 'SSE', 'SSE_IDE'].includes(service.transport) && <McpServiceOAuth name={service.name} enabled={service.enabled} />}
            </div>
            <button type="button" role="switch" aria-checked={service.enabled} aria-label={`${service.name} 服务`} disabled={changing !== null || loading}
                className="btn-secondary shrink-0" onClick={() => void setEnabled(service.name, !service.enabled)}>
                {changing === service.name ? '保存中…' : service.enabled ? '已启用' : '已关闭'}
            </button>
        </li>)}</ul>
    </section>;
}
