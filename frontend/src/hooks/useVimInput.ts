import { useCallback, useEffect, useRef, useState, type KeyboardEvent, type RefObject } from 'react';

type VimMode = 'insert' | 'normal' | 'visual';
interface Snapshot { text: string; cursor: number }
const MAX_UNDO_BYTES = 1024 * 1024;

/** A local textarea editor; it never dispatches commands or submits on its own. */
export function useVimInput(enabled: boolean, text: string, setText: (value: string) => void,
    textarea: RefObject<HTMLTextAreaElement>, owner: string) {
    const [mode, setMode] = useState<VimMode>('insert');
    const state = useRef({ pending: '', count: '', anchor: 0, visualCursor: 0, linewise: false, register: '', history: [] as Snapshot[], future: [] as Snapshot[], previous: text, internal: false });
    useEffect(() => {
        state.current = { pending: '', count: '', anchor: 0, visualCursor: 0, linewise: false, register: '', history: [], future: [], previous: text, internal: false };
        setMode('insert');
        // Text edits should not reset the editor; only preference/owner changes do.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [enabled, owner]);
    useEffect(() => {
        const current = state.current;
        if (current.previous !== text && enabled && !current.internal) {
            remember(current.history, { text: current.previous, cursor: textarea.current?.selectionStart ?? 0 });
            current.future = [];
        }
        current.previous = text;
        current.internal = false;
    }, [text, enabled, textarea]);
    const position = useCallback((start: number, end = start) => {
        requestAnimationFrame(() => textarea.current?.setSelectionRange(start, end));
    }, [textarea]);
    const handleKeyDown = useCallback((event: KeyboardEvent<HTMLTextAreaElement>) => {
        if (!enabled || event.nativeEvent.isComposing || event.keyCode === 229) return false;
        const element = event.currentTarget;
        const current = state.current;
        const cursor = mode === 'visual' ? current.visualCursor : element.selectionStart;
        const consume = () => { event.preventDefault(); return true; };
        const setCursor = (next: number) => {
            const target = Math.max(0, Math.min(text.length, next));
            if (mode === 'visual') { current.visualCursor = target; position(Math.min(current.anchor, target), Math.max(current.anchor, target)); }
            else position(target);
        };
        const edit = (next: string, target: number) => {
            remember(current.history, { text, cursor }); current.future = [];
            current.internal = true; current.previous = next; setText(next); position(target);
        };
        if (event.key === 'Escape') {
            current.pending = ''; current.count = '';
            if (mode === 'insert' && cursor > 0) position(advanceCodePoints(text, cursor, -1));
            else position(cursor);
            setMode('normal'); return consume();
        }
        if (mode === 'insert') return false;
        if (event.ctrlKey && event.key.toLowerCase() === 'r') {
            const next = current.future.pop();
            if (next) { remember(current.history, { text, cursor }); current.internal = true; current.previous = next.text; setText(next.text); position(next.cursor); }
            return consume();
        }
        if (event.ctrlKey || event.metaKey || event.altKey || ['Enter', 'Tab'].includes(event.key)) return false;
        const key = event.key;
        if (/^[1-9]$/.test(key) || (key === '0' && current.count)) {
            current.count = (current.count + key).slice(0, 3); return consume();
        }
        const count = Math.min(999, Number(current.count) || 1);
        if (key === 'u') {
            const previous = current.history.pop();
            if (previous) { remember(current.future, { text, cursor }); current.internal = true; current.previous = previous.text; setText(previous.text); position(previous.cursor); }
            current.pending = ''; current.count = ''; return consume();
        }
        const lineStart = text.lastIndexOf('\n', cursor - 1) + 1;
        const nextBreak = text.indexOf('\n', cursor);
        const lineEnd = nextBreak < 0 ? text.length : nextBreak;
        if (mode === 'visual' && ['d', 'x', 'c', 'y'].includes(key)) {
            const start = element.selectionStart; const end = element.selectionEnd;
            current.register = text.slice(start, end); current.linewise = false;
            if (key !== 'y') edit(text.slice(0, start) + text.slice(end), start);
            else position(start);
            setMode(key === 'c' ? 'insert' : 'normal'); current.count = ''; return consume();
        }
        if (!current.pending && ['i', 'a', 'I', 'A', 'o', 'O'].includes(key)) {
            setMode('insert');
            if (key === 'i') position(cursor);
            if (key === 'a') position(Math.min(cursor + 1, text.length));
            if (key === 'I') position(lineStart);
            if (key === 'A') position(lineEnd);
            if (key === 'o') edit(text.slice(0, lineEnd) + '\n' + text.slice(lineEnd), lineEnd + 1);
            if (key === 'O') edit(text.slice(0, lineStart) + '\n' + text.slice(lineStart), lineStart);
            current.count = ''; return consume();
        }
        if (!current.pending && key === 'v') { current.anchor = cursor; current.visualCursor = cursor; setMode(mode === 'visual' ? 'normal' : 'visual'); current.count = ''; return consume(); }
        if (!current.pending && key === 'x') {
            const end = advanceCodePoints(text, cursor, count);
            current.register = text.slice(cursor, end); current.linewise = false; edit(text.slice(0, cursor) + text.slice(end), cursor); current.count = ''; return consume();
        }
        if (!current.pending && (key === 'p' || key === 'P')) {
            const at = current.linewise ? (key === 'P' ? lineStart : Math.min(lineEnd + 1, text.length))
                : key === 'P' ? cursor : Math.min(advanceCodePoints(text, cursor, 1), text.length);
            let inserted = current.register.repeat(count);
            if (inserted && current.linewise) {
                if (!inserted.endsWith('\n')) inserted += '\n';
                if (key === 'p' && lineEnd === text.length) inserted = '\n' + inserted.replace(/\n$/, '');
            }
            if (inserted) edit(text.slice(0, at) + inserted + text.slice(at), current.linewise ? at : at + inserted.length - 1);
            current.count = ''; return consume();
        }
        if (!current.pending && ['d', 'c', 'y', 'g'].includes(key)) { current.pending = key; return consume(); }
        let target: number | null = null;
        if (key === 'h' || key === 'ArrowLeft') target = advanceCodePoints(text, cursor, -count);
        if (key === 'l' || key === 'ArrowRight') target = advanceCodePoints(text, cursor, count);
        if (key === '0' || key === 'Home') target = lineStart;
        if (key === '$' || key === 'End') target = lineEnd;
        if (key === 'G') target = text.length;
        if (key === 'g' && current.pending === 'g') target = 0;
        if (key === 'j' || key === 'ArrowDown' || key === 'k' || key === 'ArrowUp') {
            target = cursor; const direction = key === 'j' || key === 'ArrowDown' ? 1 : -1; const column = cursor - lineStart;
            for (let index = 0; index < count; index++) target = verticalPosition(text, target, column, direction);
        }
        if (['w', 'b', 'e'].includes(key)) {
            target = cursor;
            for (let index = 0; index < count; index++) target = wordPosition(text, target, key);
        }
        const operator = current.pending;
        if (operator && operator !== 'g' && key === operator) {
            let end = lineEnd;
            for (let index = 1; index < count && end < text.length; index++) { const next = text.indexOf('\n', end + 1); end = next < 0 ? text.length : next; }
            const start = lineStart;
            const stop = operator === 'c' ? end : Math.min(text.length, end + 1);
            current.register = text.slice(start, stop); current.linewise = operator !== 'c';
            if (operator !== 'y') edit(text.slice(0, start) + text.slice(stop), start);
            if (operator === 'c') setMode('insert');
        } else if (target !== null && operator && operator !== 'g') {
            const changeWord = operator === 'c' && key === 'w' && !/\s/u.test(text[cursor] ?? ' ');
            if (changeWord) {
                target = wordEnd(text, cursor);
                for (let index = 1; index < count; index++) target = wordPosition(text, target, 'e');
            }
            if (changeWord || key === 'e') target = advanceCodePoints(text, target, 1);
            const start = Math.min(cursor, target); const end = Math.max(cursor, target);
            current.register = text.slice(start, end); current.linewise = false;
            if (operator !== 'y') edit(text.slice(0, start) + text.slice(end), start);
            if (operator === 'c') setMode('insert');
        } else if (target !== null) setCursor(target);
        current.pending = ''; current.count = '';
        // Normal mode never falls through to native text insertion.
        return consume();
    }, [enabled, text, setText, mode, position]);
    return { mode: enabled ? mode : null, handleKeyDown };
}
function remember(history: Snapshot[], snapshot: Snapshot) {
    if (snapshot.text.length > MAX_UNDO_BYTES / 2) return;
    history.push(snapshot);
    while (history.length > 50 || history.reduce((size, item) => size + item.text.length * 2, 0) > MAX_UNDO_BYTES) history.shift();
}
function advanceCodePoints(text: string, cursor: number, count: number) {
    let position = cursor;
    for (let index = 0; index < Math.abs(count); index++) {
        if (count > 0 && position < text.length) position += text.codePointAt(position)! > 0xffff ? 2 : 1;
        else if (count < 0 && position > 0) { position--; const code = text.charCodeAt(position); if (code >= 0xdc00 && code <= 0xdfff && position > 0) position--; }
    }
    return position;
}
function verticalPosition(text: string, cursor: number, column: number, direction: number) {
    const start = text.lastIndexOf('\n', cursor - 1) + 1;
    const end = text.indexOf('\n', cursor);
    if (direction > 0) {
        if (end < 0) return cursor;
        const next = text.indexOf('\n', end + 1);
        return Math.min(end + 1 + column, next < 0 ? text.length : next);
    }
    if (start === 0) return cursor;
    const previous = text.lastIndexOf('\n', start - 2) + 1;
    return Math.min(previous + column, start - 1);
}
function wordEnd(text: string, cursor: number) {
    const kind = (index: number) => /[\p{L}\p{N}_]/u.test(String.fromCodePoint(text.codePointAt(index) ?? 32));
    let target = cursor;
    while (target < text.length) {
        const next = advanceCodePoints(text, target, 1);
        if (next >= text.length || /\s/u.test(text[next]) || kind(next) !== kind(target)) break;
        target = next;
    }
    return target;
}
function wordPosition(text: string, cursor: number, direction: string) {
    const word = (index: number) => /[\p{L}\p{N}_]/u.test(String.fromCodePoint(text.codePointAt(index) ?? 32));
    if (direction === 'b') {
        let target = advanceCodePoints(text, cursor, -1);
        while (target > 0 && /\s/u.test(text[target])) target = advanceCodePoints(text, target, -1);
        const kind = word(target);
        while (target > 0) { const previous = advanceCodePoints(text, target, -1); if (/\s/u.test(text[previous]) || word(previous) !== kind) break; target = previous; }
        return target;
    }
    if (direction === 'e') {
        let target = advanceCodePoints(text, cursor, 1);
        while (target < text.length && /\s/u.test(text[target])) target = advanceCodePoints(text, target, 1);
        if (target >= text.length) return text.length;
        return wordEnd(text, target);
    }
    let target = cursor;
    const kind = word(target);
    while (target < text.length && !/\s/u.test(text[target]) && word(target) === kind) target = advanceCodePoints(text, target, 1);
    while (target < text.length && /\s/u.test(text[target])) target = advanceCodePoints(text, target, 1);
    return target;
}
