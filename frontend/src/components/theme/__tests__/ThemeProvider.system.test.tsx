import { act, render } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import type * as Monaco from 'monaco-editor';
import { ThemeProvider } from '../ThemeProvider';
import { useConfigStore } from '@/store/configStore';
import { ensureZkMonacoThemes } from '@/styles/zkMonaco';

afterEach(() => vi.unstubAllGlobals());
it('updates mounted Monaco editors when the OS changes under system preference', () => {
    let dark = false;
    const listeners = new Set<() => void>();
    vi.stubGlobal('matchMedia', vi.fn((query: string) => ({
        get matches() { return query.includes('color-scheme') && dark; },
        media: query, addEventListener: (_: string, callback: () => void) => { if (query.includes('color-scheme')) listeners.add(callback); },
        removeEventListener: (_: string, callback: () => void) => listeners.delete(callback),
    })));
    useConfigStore.getState().setTheme({ mode: 'system' });
    const setTheme = vi.fn();
    ensureZkMonacoThemes({ editor: { defineTheme: vi.fn(), setTheme } } as unknown as typeof Monaco);
    render(<ThemeProvider>{null}</ThemeProvider>);
    act(() => { dark = true; listeners.forEach(listener => listener()); });
    expect(document.documentElement).toHaveClass('dark');
    expect(setTheme).toHaveBeenLastCalledWith('zk-dark');
    act(() => { dark = false; listeners.forEach(listener => listener()); });
    expect(setTheme).toHaveBeenLastCalledWith('zk-light');
});
