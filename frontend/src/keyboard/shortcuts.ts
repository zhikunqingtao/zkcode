export const SHORTCUT_ACTIONS = [
    { id: 'chat:submit', label: '发送消息', context: 'chat', defaults: ['enter'] },
    { id: 'chat:commandPalette', label: '命令面板', context: 'global', defaults: ['ctrl+k', 'meta+k'] },
    { id: 'chat:focus', label: '聚焦输入框', context: 'global', defaults: ['ctrl+shift+i', 'meta+shift+i'] },
    { id: 'app:settings', label: '打开设置', context: 'global', defaults: ['ctrl+comma', 'meta+comma'] },
    { id: 'app:keybindings', label: '快捷键与 Vim', context: 'global', defaults: ['ctrl+slash', 'meta+slash'] },
] as const;
export type ShortcutAction = typeof SHORTCUT_ACTIONS[number]['id'];
export interface EditorPreferences { vimEnabled: boolean; keybindings: Partial<Record<ShortcutAction, string>> }
export const DEFAULT_EDITOR_PREFERENCES: EditorPreferences = { vimEnabled: false, keybindings: {} };
const MODIFIERS = ['ctrl', 'alt', 'shift', 'meta'];
const RESERVED = new Set(['c', 'v', 'x', 'z']);
const KEY_NAMES: Record<string, string> = { control: 'ctrl', cmd: 'meta', command: 'meta', option: 'alt', mod: 'meta', escape: 'escape', ' ': 'space', arrowup: 'up', arrowdown: 'down', arrowleft: 'left', arrowright: 'right', ',': 'comma', '/': 'slash' };
export function normalizeShortcut(input: string): string {
    return input.trim().toLowerCase().split(/\s+/).filter(Boolean).map(step => {
        const tokens = step.split('+').map(token => KEY_NAMES[token] ?? token);
        const key = tokens.pop() ?? '';
        if (!key || tokens.some(token => !MODIFIERS.includes(token)) || new Set(tokens).size !== tokens.length) throw new Error('按键格式无效');
        if (!/^[a-z0-9]$/.test(key) && !['enter', 'space', 'up', 'down', 'left', 'right', 'home', 'end', 'pageup', 'pagedown', 'comma', 'slash', 'backspace', 'delete'].includes(key)) throw new Error('不支持此按键');
        return [...MODIFIERS.filter(token => tokens.includes(token)), key].join('+');
    }).join(' ');
}
export function effectiveShortcuts(preferences: EditorPreferences, action: ShortcutAction): readonly string[] {
    const configured = preferences.keybindings[action];
    return configured === undefined ? SHORTCUT_ACTIONS.find(item => item.id === action)!.defaults : configured ? [configured] : [];
}
export function validateEditorPreferences(value: unknown): EditorPreferences {
    if (!value || typeof value !== 'object') throw new Error('快捷键配置无效');
    const candidate = value as EditorPreferences;
    if (typeof candidate.vimEnabled !== 'boolean' || !candidate.keybindings || typeof candidate.keybindings !== 'object' || Array.isArray(candidate.keybindings)) throw new Error('快捷键配置无效');
    const preferences: EditorPreferences = { vimEnabled: candidate.vimEnabled, keybindings: {} };
    for (const [action, shortcut] of Object.entries(candidate.keybindings)) {
        if (!SHORTCUT_ACTIONS.some(item => item.id === action) || typeof shortcut !== 'string' || shortcut.length > 96) throw new Error('未知快捷键动作或无效按键');
        preferences.keybindings[action as ShortcutAction] = normalizeShortcut(shortcut);
    }
    const used: string[][] = [];
    for (const action of SHORTCUT_ACTIONS) {
        for (const shortcut of effectiveShortcuts(preferences, action.id)) {
            const steps = shortcut.split(' ');
            if (steps.length > 2) throw new Error('最多支持两个按键组合的和弦');
            for (const step of steps) {
                const tokens = step.split('+');
                const key = tokens.pop()!;
                if (!tokens.some(token => ['ctrl', 'alt', 'meta'].includes(token)) && !(action.id === 'chat:submit' && step === 'enter')) throw new Error('快捷键须含 Ctrl、⌘ 或 Alt；发送可用 Enter');
                if (RESERVED.has(key) && tokens.some(token => token === 'ctrl' || token === 'meta')) throw new Error('复制、粘贴、剪切、撤销与 Ctrl+C 中断保留');
            }
            if (used.some(existing => existing.every((step, index) => steps[index] === step) || steps.every((step, index) => existing[index] === step))) throw new Error('快捷键重复，或与另一快捷键的和弦前缀冲突');
            used.push(steps);
        }
    }
    return preferences;
}
export function keyboardCombo(event: KeyboardEvent): string {
    const key = KEY_NAMES[event.key.toLowerCase()] ?? event.key.toLowerCase();
    if (MODIFIERS.includes(key)) return '';
    return [event.ctrlKey && 'ctrl', event.altKey && 'alt', event.shiftKey && 'shift', event.metaKey && 'meta', key].filter(Boolean).join('+');
}
