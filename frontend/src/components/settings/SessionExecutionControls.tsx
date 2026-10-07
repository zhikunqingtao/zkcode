import { useEffect } from 'react';
import { SessionReplService } from './SessionReplService';
import { useSessionStore } from '@/store/sessionStore';
import { useExecutionPreferencesStore } from '@/store/executionPreferencesStore';
import { isMergeSource } from '@/store/sessionMergeStore';

export function SessionExecutionControls() {
    const purpose = useSessionStore(state => state.purpose);
    const sessionId = useSessionStore(state => state.sessionId);
    const model = useSessionStore(state => state.model);
    const entry = useExecutionPreferencesStore(state => sessionId ? state.sessions[sessionId] : undefined);
    const { load, update } = useExecutionPreferencesStore();
    useEffect(() => { if (sessionId && purpose !== 'mcp') void load(sessionId); }, [sessionId, model, load, purpose]);
    const disabled = !sessionId || !entry?.value || entry.loading || entry.saving || isMergeSource(sessionId);
    if (purpose === 'mcp') return <p className="mt-4 text-sm text-t2">MCP 专用会话使用外部工具执行上下文，不设置聊天模型或推理强度。</p>;
    return <div className="mt-4 space-y-3 text-sm text-t2">
        <p>当前会话执行设置 · 下一轮生效</p>
        <label className="block">推理强度
            <select aria-label="当前会话推理强度" className="mt-1 block w-full rounded border border-hairline bg-sunken2 p-2 text-t1" disabled={disabled} value={entry?.value?.effort ?? 'auto'} onChange={event => { if (sessionId) void update(sessionId, { effort: event.target.value }); }}>
                <option value="auto">自动（保留模型默认）</option>
                {entry?.value?.effort !== 'auto' && entry?.value && !entry.value.supportedEfforts.includes(entry.value.effort) && <option disabled value={entry.value.effort}>{entry.value.effort}（当前不可用）</option>}
                {entry?.value?.supportedEfforts.map(effort => <option key={effort} value={effort}>{effort}</option>)}
            </select>
        </label>
        <label className="flex items-center gap-2"><input type="checkbox" checked={entry?.value?.fast ?? false} disabled={disabled || (!entry?.value?.fast && !entry?.value?.fastAvailable)} onChange={event => { if (sessionId) void update(sessionId, { fast: event.target.checked }); }} />快模型路由</label>
        <p className="text-xs text-t3">{entry?.value?.fastAvailable ? `当前有效模型：${entry.value.effectiveModel}。原会话模型和新会话默认保持独立。` : '需先配置本机快模型；不会自动增加按量 Key 或付费候选。'}</p>
        {entry?.value?.validationError && <p role="alert" className="text-err">{entry.value.validationError} <button type="button" className="underline" disabled={disabled} onClick={() => { if (sessionId) void update(sessionId, { effort: 'auto', fast: false }); }}>恢复当前会话默认执行</button></p>}
        {entry?.error && <p role="alert" className="text-err">{entry.error} <button type="button" className="underline" onClick={() => { if (sessionId) void load(sessionId); }}>重新加载</button></p>}
        <SessionReplService />
    </div>;
}
