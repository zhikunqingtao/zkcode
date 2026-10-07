import { act, cleanup, fireEvent, render, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { CHORD_TIMEOUT_MS, isContextActive, useKeybinding, useRegisterKeybindingContext } from './useKeybinding';
import { validateEditorPreferences } from '@/keyboard/shortcuts';

afterEach(() => { cleanup(); vi.useRealTimers(); });
describe('real shortcut dispatcher', () => {
    it('runs a two-stroke chord once, expires it, and releases timers on unmount', () => {
        vi.useFakeTimers();
        const handler = vi.fn();
        const hook = renderHook(() => useKeybinding([{ key: 'ctrl+k ctrl+g', action: 'palette', context: 'global', handler }]));
        fireEvent.keyDown(window, { key: 'k', ctrlKey: true });
        expect(hook.result.current.pendingChord).toBe('ctrl+k');
        fireEvent.keyDown(window, { key: 'g', ctrlKey: true });
        expect(handler).toHaveBeenCalledTimes(1);
        expect(hook.result.current.pendingChord).toBeNull();
        fireEvent.keyDown(window, { key: 'k', ctrlKey: true });
        act(() => vi.advanceTimersByTime(CHORD_TIMEOUT_MS));
        fireEvent.keyDown(window, { key: 'g', ctrlKey: true });
        expect(handler).toHaveBeenCalledTimes(1);
        fireEvent.keyDown(window, { key: 'k', ctrlKey: true });
        hook.unmount();
        expect(vi.getTimerCount()).toBe(0);
    });
    it('prioritizes active context and preserves composing, consumed and repeated events', () => {
        const local = vi.fn(); const global = vi.fn();
        renderHook(() => {
            useRegisterKeybindingContext('settings');
            return useKeybinding([
                { key: 'ctrl+j', action: 'global', context: 'global', handler: global },
                { key: 'ctrl+j', action: 'local', context: 'settings', handler: local },
            ]);
        });
        fireEvent.keyDown(window, { key: 'j', ctrlKey: true, isComposing: true });
        fireEvent.keyDown(window, { key: 'j', ctrlKey: true, repeat: true });
        const consumed = new KeyboardEvent('keydown', { key: 'j', ctrlKey: true, cancelable: true });
        consumed.preventDefault(); fireEvent(window, consumed);
        expect(local).not.toHaveBeenCalled();
        fireEvent.keyDown(window, { key: 'j', ctrlKey: true });
        expect(local).toHaveBeenCalledTimes(1); expect(global).not.toHaveBeenCalled();
    });
    it('counts multiple mounted owners of the same context', () => {
        const Context = () => { useRegisterKeybindingContext('chat'); return null; };
        const first = render(<Context />); const second = render(<Context />);
        first.unmount(); expect(isContextActive('chat')).toBe(true);
        second.unmount(); expect(isContextActive('chat')).toBe(false);
    });
    it('rejects prefix conflicts and reserved editing/interrupt keys', () => {
        for (const shortcut of ['ctrl+k ctrl+j', 'ctrl+c', 'meta+v', 'j']) {
            expect(() => validateEditorPreferences({ vimEnabled: false, keybindings: { 'chat:focus': shortcut } })).toThrow();
        }
        expect(validateEditorPreferences({ vimEnabled: true, keybindings: { 'chat:commandPalette': 'CTRL+K CTRL+G' } }).keybindings['chat:commandPalette']).toBe('ctrl+k ctrl+g');
    });
});
