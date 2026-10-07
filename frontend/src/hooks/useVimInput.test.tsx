import { useRef, useState } from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { useVimInput } from './useVimInput';

function Editor({ enabled = true, initial = 'hello world' }: { enabled?: boolean; initial?: string }) {
    const [text, setText] = useState(initial); const ref = useRef<HTMLTextAreaElement>(null);
    const vim = useVimInput(enabled, text, setText, ref, 'session-a');
    return <><output>{vim.mode ?? 'disabled'}</output><textarea aria-label="editor" ref={ref} value={text} onChange={event => setText(event.target.value)} onKeyDown={vim.handleKeyDown} /></>;
}
let input: HTMLTextAreaElement;
function key(value: string, options: object = {}) {
    fireEvent.keyDown(input, { key: value, ...options });
    act(() => vi.advanceTimersByTime(20));
}
beforeEach(() => vi.useFakeTimers());
afterEach(() => { cleanup(); vi.useRealTimers(); });
it('supports real word deletion, undo/redo and returns to insertion', () => {
    render(<Editor />); input = screen.getByLabelText('editor'); input.setSelectionRange(0, 0);
    key('Escape'); expect(screen.getByText('normal')).toBeTruthy();
    key('d'); key('w'); expect(input.value).toBe('world');
    key('u'); expect(input.value).toBe('hello world');
    key('r', { ctrlKey: true }); expect(input.value).toBe('world');
    key('i'); expect(screen.getByText('insert')).toBeTruthy();
    fireEvent.change(input, { target: { value: '你好 world' } }); expect(input.value).toBe('你好 world');
});
it('yanks and pastes full lines, then visual motions extend an actual selection', () => {
    render(<Editor initial={'one\ntwo\nthree'} />); input = screen.getByLabelText('editor'); input.setSelectionRange(0, 0);
    key('Escape'); key('y'); key('y'); key('p'); expect(input.value).toBe('one\none\ntwo\nthree');
    key('g'); key('g'); key('v'); key('l'); key('l');
    expect([input.selectionStart, input.selectionEnd]).toEqual([0, 2]);
    key('x'); expect(input.value.startsWith('e\n')).toBe(true);
    key('u'); expect(input.value.startsWith('one\n')).toBe(true);
});
it('does not corrupt non-BMP characters and protects IME/native editing while disabled', () => {
    const view = render(<Editor initial="😀文字" />); input = screen.getByLabelText('editor'); input.setSelectionRange(0, 0);
    key('Escape'); key('x'); expect(input.value).toBe('文字');
    key('i'); key('Escape', { isComposing: true }); expect(screen.getByText('insert')).toBeTruthy();
    view.rerender(<Editor enabled={false} initial="ignored" />);
    const event = new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true });
    fireEvent(input, event); expect(event.defaultPrevented).toBe(false);
    expect(screen.getByText('disabled')).toBeTruthy();
});
it('changes the word without consuming its separator and supports counted line deletion', () => {
    render(<Editor initial={'hello world\ntwo\nthree'} />); input = screen.getByLabelText('editor'); input.setSelectionRange(0, 0);
    key('Escape'); key('c'); key('w'); expect(input.value).toBe(' world\ntwo\nthree');
    key('Escape'); key('2'); key('d'); key('d'); expect(input.value).toBe('three');
});
it('end-of-word motion advances across successive words', () => {
    render(<Editor />); input = screen.getByLabelText('editor'); input.setSelectionRange(0, 0);
    key('Escape'); key('e'); expect(input.selectionStart).toBe(4);
    key('e'); expect(input.selectionStart).toBe(10);
});
it('cw changes a one-character word without eating the next word', () => {
    render(<Editor initial="a b" />); input = screen.getByLabelText('editor'); input.setSelectionRange(0, 0);
    key('Escape'); key('c'); key('w'); expect(input.value).toBe(' b');
});
