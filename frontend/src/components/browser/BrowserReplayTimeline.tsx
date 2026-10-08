/**
 * BrowserReplayTimeline — zkcode v1.5 升级项 A MVP。
 *
 * 会话级浏览器语义快照时间线：
 *   · 通过 GET /api/browser/replay/{sessionId} 拉取持久快照序列
 *   · 复用 <Drawer> 作为承载容器，右侧滑出
 *   · 左侧列表：时间戳 + URL/title + 节点/交互统计 + 可选缩略图
 *   · 点击某帧展开底部交互元素表 + 顶部 5 层语义树预览
 *
 * 数据模型对齐后端 BrowserSnapshot record：
 *   { snapshotId, sessionId, capturedAt, url, title, selector,
 *     nodeCount, interactive[], tree{}, screenshotBase64 }
 *
 * MVP 约束：
 *   · 不主动轮询（由父组件或用户触发 refresh）；避免 WebSocket 带宽浪费
 *   · 后端使用 workspace `.zk/browser-replay` 原子 JSON，带容量与 retention 上限
 */

import React, { useCallback, useEffect, useMemo, useState, useRef } from 'react';
import { Drawer } from '@/components/layout/Drawer';
import { RefreshCw, Image as ImageIcon, ChevronDown, ChevronRight, Trash2 } from 'lucide-react';

export interface BrowserSnapshot {
    snapshotId: string;
    sessionId: string;
    capturedAt: string;
    url: string | null;
    title: string | null;
    selector: string | null;
    nodeCount: number;
    interactive: Array<{ role: string; name?: string; value?: string; disabled?: boolean }>;
    tree: Record<string, unknown> | null;
    screenshotBase64: string | null;
    captureStatus?: 'complete' | 'partial' | 'failed';
    components?: Record<string, {
        status: 'ok' | 'failed' | 'not_requested';
        error_code?: string;
        reason?: string;
        truncated?: boolean;
    }>;
}

interface BrowserReplayTimelineProps {
    open: boolean;
    onClose: () => void;
    sessionId: string;
    /** 默认 360，覆盖移动端/桌面端布局 */
    width?: number;
    inline?: boolean;
}

const BrowserReplayTimeline: React.FC<BrowserReplayTimelineProps> = ({
    open,
    onClose,
    sessionId,
    width = 420,
    inline = false,
}) => {
    const [snapshots, setSnapshots] = useState<BrowserSnapshot[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [selectedId, setSelectedId] = useState<string | null>(null);

    const request = useRef<AbortController | null>(null);
    const epoch = useRef(0);

    const load = useCallback(async (method: 'GET' | 'DELETE') => {
        if (!sessionId || !open) return;
        request.current?.abort();
        const controller = new AbortController();
        request.current = controller;
        const generation = ++epoch.current;
        setLoading(true);
        setError(null);
        try {
            const response = await fetch(`/api/browser/replay/${encodeURIComponent(sessionId)}`, {
                method, headers: { 'X-Session-Id': sessionId }, signal: controller.signal,
            });
            const data: unknown = await response.json();
            if (controller.signal.aborted || generation !== epoch.current) return;
            if (!response.ok) {
                if (method === 'GET' && response.status === 404 && hasCode(data, 'REPLAY_NOT_FOUND')) {
                    setSnapshots([]);
                    setSelectedId(null);
                    return;
                }
                if (hasCode(data, 'EPHEMERAL_OPERATION_UNSUPPORTED')) {
                    throw new Error('临时会话不保存磁盘时间线；请查看本轮浏览器工具的证据。');
                }
                throw new Error(`HTTP ${response.status}：浏览器时间线操作未完成`);
            }
            if (method === 'DELETE') {
                if (!data || typeof data !== 'object' || !('status' in data) || data.status !== 'deleted') {
                    throw new Error('服务端未确认时间线已清空，请刷新后核实');
                }
                setSnapshots([]);
                setSelectedId(null);
            } else {
                if (!Array.isArray(data) || data.some(item => !validSnapshot(item, sessionId))) {
                    throw new Error('浏览器时间线返回了无效或跨会话数据');
                }
                setSnapshots(data);
                setSelectedId(previous => data.some(item => item.snapshotId === previous) ? previous : null);
            }
        } catch (error) {
            if (!controller.signal.aborted && generation === epoch.current) {
                setError(error instanceof Error ? error.message : String(error));
            }
        } finally {
            if (!controller.signal.aborted && generation === epoch.current) setLoading(false);
        }
    }, [sessionId, open]);

    const fetchTimeline = useCallback(() => { void load('GET'); }, [load]);
    const clearTimeline = useCallback(() => {
        if (window.confirm('清空当前会话的浏览器快照时间线？此操作不会删除消息中的证据。')) void load('DELETE');
    }, [load]);

    const cancelPending = useCallback(() => {
        ++epoch.current;
        request.current?.abort();
    }, []);

    useEffect(() => {
        setSnapshots([]);
        setSelectedId(null);
        setError(null);
        setLoading(false);
        if (open && sessionId) void load('GET');
        return cancelPending;
    }, [open, sessionId, load, cancelPending]);

    const selected = useMemo(
        () => snapshots.find((s) => s.snapshotId === selectedId) ?? null,
        [snapshots, selectedId],
    );

    const content = (
        <>
            <div className="flex items-center justify-between px-4 py-3 border-b border-[var(--v2-border-hairline)]">
                <div className="flex flex-col">
                    <span className="text-sm font-semibold text-[var(--v2-text-1)]">
                        浏览器快照时间线
                    </span>
                    <span className="text-[13px] text-[var(--v2-text-2)]">
                        session: {sessionId.slice(0, 12)}… · {snapshots.length} 帧
                    </span>
                </div>
                <div className="flex items-center gap-2">
                    <button
                        type="button"
                        onClick={fetchTimeline}
                        disabled={loading}
                        className="panel-control p-1.5 rounded-sm hover:bg-[var(--v2-bg-sunken)] disabled:opacity-50"
                        title="刷新"
                    >
                        <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
                    </button>
                    <button
                        type="button"
                        onClick={clearTimeline}
                        disabled={loading || snapshots.length === 0}
                        className="panel-control p-1.5 rounded-sm hover:bg-[var(--v2-bg-sunken)] disabled:opacity-50"
                        title="清空"
                    >
                        <Trash2 size={14} />
                    </button>
                </div>
            </div>

            {error && (
                <div className="px-4 py-2 text-[13px] text-err bg-errsoft border-b border-err">
                    {error}
                </div>
            )}

            <div className="flex-1 overflow-y-auto">
                {snapshots.length === 0 && !loading && !error && (
                    <div className="p-6 text-center text-[13px] text-[var(--v2-text-2)]">
                        暂无快照。可在对话中输入
                        <code className="mx-1 px-1 bg-[var(--v2-bg-sunken)] rounded-sm">/browser-snapshot</code>
                        触发一次采集。
                    </div>
                )}
                <ul className="divide-y divide-[var(--v2-border-hairline)]">
                    {snapshots.map((snap) => (
                        <SnapshotRow
                            key={snap.snapshotId}
                            snapshot={snap}
                            expanded={snap.snapshotId === selectedId}
                            onToggle={() =>
                                setSelectedId((prev) => (prev === snap.snapshotId ? null : snap.snapshotId))
                            }
                        />
                    ))}
                </ul>
            </div>

            {selected && (
                <div className="border-t border-[var(--v2-border-hairline)] max-h-64 overflow-y-auto p-3 bg-[var(--v2-bg-sunken)]/30">
                    <CaptureDetail snapshot={selected} />
                    <InteractiveList interactive={selected.interactive} />
                </div>
            )}
        </>
    );
    return inline ? <section className="flex h-full min-h-0 flex-col" aria-label="浏览器快照时间线">{content}</section>
        : <Drawer open={open} onClose={onClose} width={width} side="right">{content}</Drawer>;
};

function CaptureDetail({ snapshot }: { snapshot: BrowserSnapshot }) {
    const incomplete = Object.entries(snapshot.components ?? {})
        .filter(([, component]) => component.status === 'failed' || component.truncated);
    return <>
        {(snapshot.captureStatus === 'partial' || snapshot.captureStatus === 'failed' || incomplete.length > 0) && (
            <div role="alert" className="mb-3 text-[13px] text-warnstrong">
                {snapshot.captureStatus === 'failed' ? '快照采集失败' : '快照包含不完整内容'}
                {incomplete.map(([name, component]) => <div key={name}>
                    {name}：{component.truncated ? '内容已截断' : '采集失败'}{component.error_code ? ` (${component.error_code})` : ''}
                </div>)}
            </div>
        )}
        {snapshot.tree?.source === 'safe_dom_v1' && typeof snapshot.tree.safe_dom === 'string' && (
            <div className="mb-3">
                <div className="text-[13px] font-semibold text-[var(--v2-text-2)] mb-2">安全 DOM 快照</div>
                <pre className="whitespace-pre-wrap break-words text-[13px] text-[var(--v2-text-1)]">{snapshot.tree.safe_dom}</pre>
            </div>
        )}
    </>;
}

function hasCode(value: unknown, code: string): boolean {
    return !!value && typeof value === 'object' && 'code' in value && value.code === code;
}
function validSnapshot(value: unknown, sessionId: string): value is BrowserSnapshot {
    if (!value || typeof value !== 'object') return false;
    const frame = value as Partial<BrowserSnapshot>;
    return frame.sessionId === sessionId && typeof frame.snapshotId === 'string'
        && typeof frame.capturedAt === 'string' && typeof frame.nodeCount === 'number'
        && Array.isArray(frame.interactive)
        && frame.interactive.every(item => !!item && typeof item.role === 'string')
        && (frame.url === null || typeof frame.url === 'string')
        && (frame.title === null || typeof frame.title === 'string')
        && (frame.captureStatus === undefined || ['complete', 'partial', 'failed'].includes(frame.captureStatus))
        && (frame.components === undefined || (!!frame.components && typeof frame.components === 'object'
            && Object.values(frame.components).every(component => !!component
                && ['ok', 'failed', 'not_requested'].includes(component.status)
                && (component.error_code === undefined || typeof component.error_code === 'string')
                && (component.truncated === undefined || typeof component.truncated === 'boolean'))))
        && (frame.screenshotBase64 === null || typeof frame.screenshotBase64 === 'string');
}

interface SnapshotRowProps {
    snapshot: BrowserSnapshot;
    expanded: boolean;
    onToggle: () => void;
}

const SnapshotRow: React.FC<SnapshotRowProps> = ({ snapshot, expanded, onToggle }) => {
    const ts = useMemo(() => {
        try {
            return new Date(snapshot.capturedAt).toLocaleTimeString();
        } catch {
            return snapshot.capturedAt;
        }
    }, [snapshot.capturedAt]);

    return (
        <li className="px-3 py-2 hover:bg-[var(--v2-bg-sunken)]/50 cursor-pointer" onClick={onToggle}>
            <div className="flex items-start gap-2">
                <div className="mt-0.5 text-[var(--v2-text-2)]">
                    {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                </div>
                <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 text-[13px] text-[var(--v2-text-2)] mb-1">
                        <span className="font-mono">{ts}</span>
                        <span className="opacity-60">·</span>
                        <span>
                            {snapshot.nodeCount} nodes
                        </span>
                        <span className="opacity-60">·</span>
                        <span>{snapshot.interactive?.length ?? 0} interactive</span>
                        {snapshot.screenshotBase64 && (
                            <ImageIcon size={12} className="opacity-60" />
                        )}
                    </div>
                    <div className="text-sm text-[var(--v2-text-1)] truncate">
                        {snapshot.title || snapshot.url || '(untitled)'}
                    </div>
                    {snapshot.url && (
                        <div className="text-[13px] text-[var(--v2-text-2)] truncate font-mono">
                            {snapshot.url}
                        </div>
                    )}
                </div>
            </div>
            {expanded && snapshot.screenshotBase64 && (
                <div className="mt-2 ml-6">
                    <img
                        src={`data:image/png;base64,${snapshot.screenshotBase64}`}
                        alt="snapshot"
                        className="max-w-full max-h-40 rounded-sm border border-[var(--v2-border-hairline)]"
                    />
                </div>
            )}
        </li>
    );
};

interface InteractiveListProps {
    interactive: BrowserSnapshot['interactive'];
}

const InteractiveList: React.FC<InteractiveListProps> = ({ interactive }) => {
    if (!interactive || interactive.length === 0) {
        return (
            <div className="text-[13px] text-[var(--v2-text-2)]">未抽取到交互元素。</div>
        );
    }
    return (
        <div>
            <div className="text-[13px] font-semibold text-[var(--v2-text-2)] mb-2">
                交互元素 ({interactive.length})
            </div>
            <ul className="space-y-1 text-[13px] font-mono">
                {interactive.slice(0, 50).map((it, idx) => (
                    <li key={idx} className="flex gap-2">
                        <span className="text-accent2-ink min-w-[64px]">{it.role}</span>
                        <span className="text-[var(--v2-text-1)] truncate flex-1">
                            {it.name || '(no name)'}
                        </span>
                        {it.disabled && (
                            <span className="text-[var(--v2-text-2)] opacity-60">disabled</span>
                        )}
                    </li>
                ))}
                {interactive.length > 50 && (
                    <li className="text-[var(--v2-text-2)] opacity-60">
                        … 还有 {interactive.length - 50} 项
                    </li>
                )}
            </ul>
        </div>
    );
};

export default BrowserReplayTimeline;
