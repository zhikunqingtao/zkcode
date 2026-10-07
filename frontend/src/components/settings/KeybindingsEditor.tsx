import { useEffect, useState } from 'react';
import { Button, Input } from '@/components/ui';
import { DEFAULT_EDITOR_PREFERENCES, SHORTCUT_ACTIONS, type EditorPreferences } from '@/keyboard/shortcuts';
import { useEditorPreferencesStore } from '@/store/editorPreferencesStore';

export function KeybindingsEditor() {
    const { preferences, loaded, loading, saving, error, load, save } = useEditorPreferencesStore();
    const [draft, setDraft] = useState<EditorPreferences>(preferences);
    const [saved, setSaved] = useState(false);
    useEffect(() => { void load(); }, [load]);
    useEffect(() => { setDraft(preferences); }, [preferences]);
    const disabled = loading || saving || !loaded;
    return <div className="space-y-4 text-sm text-t2">
        <p>本机编辑器设置，影响所有项目。单键组合如 <code>meta+k</code>；和弦如 <code>ctrl+k ctrl+g</code>。⌘ 使用 meta，和弦间按空格。</p>
        <label className="flex items-center gap-2">
            <input type="checkbox" checked={draft.vimEnabled} disabled={disabled} onChange={event => { setDraft({ ...draft, vimEnabled: event.target.checked }); setSaved(false); }} />
            输入框 Vim 模式（默认关闭）
        </label>
        <p className="text-xs">Vim 仅作用聊天输入框：Esc 进入 Normal，i/a/o 插入，h/j/k/l、w/b/e、0/$、gg/G 移动，v 选区，x/dd/dw、c/cc/cw 编辑，yy/p 复制粘贴，u/Ctrl+R 撤销重做。输入法组合时不拦截按键；Enter 沿发送设置。</p>
        {SHORTCUT_ACTIONS.map(action => <label key={action.id} className="block space-y-1">
            <span>{action.label}</span>
            <div className="flex gap-2">
                <Input aria-label={`${action.label}快捷键`} disabled={disabled} value={draft.keybindings[action.id] ?? action.defaults[0]} onChange={event => {
                    setDraft({ ...draft, keybindings: { ...draft.keybindings, [action.id]: event.target.value } }); setSaved(false);
                }} />
                <Button disabled={disabled} onClick={() => {
                    const keybindings = { ...draft.keybindings }; delete keybindings[action.id];
                    setDraft({ ...draft, keybindings }); setSaved(false);
                }}>默认</Button>
            </div>
            <span className="text-xs text-t3">{draft.keybindings[action.id] === undefined ? `默认：${action.defaults.join(' / ')}` : draft.keybindings[action.id] ? '自定义；保存后生效' : '已禁用此动作的快捷键'}</span>
        </label>)}
        <p className="text-xs">留空可禁用；复制／粘贴／剪切／撤销、Shift+Enter 换行和 Ctrl+C 立即停止保留。弹窗和输入法优先处理按键。</p>
        {error && <p role="alert" className="text-err">{error} <button type="button" onClick={() => void load()} className="underline">重新加载</button></p>}
        {saved && <p role="status">编辑器设置已保存</p>}
        <div className="flex gap-2">
            <Button variant="primary" disabled={disabled} onClick={async () => setSaved(await save(draft))}>{saving ? '保存中…' : '保存编辑器设置'}</Button>
            <Button disabled={disabled} onClick={() => { setDraft(DEFAULT_EDITOR_PREFERENCES); setSaved(false); }}>恢复默认草稿</Button>
        </div>
    </div>;
}
