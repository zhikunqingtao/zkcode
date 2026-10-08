import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { HooksEditor } from './HooksEditor';
import { useSessionStore } from '@/store/sessionStore';
vi.mock('@/components/ui', () => ({ Dialog: ({ children }: { children: React.ReactNode }) => <div>{children}</div>, Button: ({ children, variant: _variant, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: string }) => <button {...props}>{children}</button> }));
const original = { content: '# original', revision: 'v1', path: '.zk/hooks.toml', hookCount: 0, validationError: null, events: [] };
const response = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status });
beforeEach(() => useSessionStore.setState({ sessionId: 'session' }));
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

describe('HooksEditor', () => {
    it('shows the named validation diagnostic as text for an invalid loaded configuration', async () => {
        const diagnostic = "HOOK_CONFIG_INVALID: hook '<img src=x onerror=alert(1)>' event PRE_TOOL_EXECUTION: HTTP_ROLE_UNSUPPORTED";
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({ ...original, validationError: 'HOOK_CONFIG_INVALID', validationMessage: diagnostic })));
        const { container } = render(<HooksEditor sessionId="session" onClose={() => {}} />);
        expect(await screen.findByRole('status')).toHaveTextContent(diagnostic);
        expect(container.querySelector('img')).toBeNull();
        expect(screen.getByRole('textbox')).toHaveValue('# original');
    });
    it('keeps an invalid save draft and displays its named server diagnostic', async () => {
        const diagnostic = "HOOK_CONFIG_INVALID: hook 'my-hook' event RUN_START: HOOK_MATCHER_INVALID";
        vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(response(original)).mockResolvedValueOnce(response({ code: 'HOOK_CONFIG_INVALID', message: diagnostic }, 400)));
        vi.spyOn(window, 'confirm').mockReturnValue(true);
        render(<HooksEditor sessionId="session" onClose={() => {}} />);
        await waitFor(() => expect(screen.getByRole('textbox')).toHaveValue('# original'));
        fireEvent.change(screen.getByRole('textbox'), { target: { value: '# invalid draft' } });
        fireEvent.click(screen.getByRole('button', { name: '保存配置' }));
        expect(await screen.findByRole('alert')).toHaveTextContent(diagnostic);
        expect(screen.getByRole('textbox')).toHaveValue('# invalid draft');
        expect(screen.queryByText('配置已保存；未执行 Hook。')).not.toBeInTheDocument();
    });
    it('saves a confirmed exact revision and never executes the saved hook', async () => {
        const fetcher = vi.fn().mockResolvedValueOnce(response(original)).mockResolvedValueOnce(response({ ...original, content: '# changed', revision: 'v2' }));
        vi.stubGlobal('fetch', fetcher);
        const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false);
        render(<HooksEditor sessionId="session" onClose={() => {}} />);
        await waitFor(() => expect(screen.getByRole('textbox')).toHaveValue('# original'));
        fireEvent.change(screen.getByRole('textbox'), { target: { value: '# changed' } });
        fireEvent.click(screen.getByRole('button', { name: '保存配置' }));
        expect(fetcher).toHaveBeenCalledTimes(1);
        confirm.mockReturnValue(true);
        fireEvent.click(screen.getByRole('button', { name: '保存配置' }));
        expect(await screen.findByRole('status')).toHaveTextContent('配置已保存；未执行 Hook');
        expect(fetcher).toHaveBeenCalledTimes(2);
        expect(fetcher.mock.calls[1]).toEqual(['/api/sessions/session/hooks', expect.objectContaining({ method: 'PUT', headers: { 'Content-Type': 'application/json', 'X-Session-Id': 'session' }, body: JSON.stringify({ revision: 'v1', content: '# changed', confirmed: true }) })]);
    });
    it('keeps the draft after CAS refusal and requires explicit discard to reload', async () => {
        const fetcher = vi.fn().mockResolvedValueOnce(response(original)).mockResolvedValueOnce(response({ code: 'HOOK_CONFIG_CHANGED' }, 409));
        vi.stubGlobal('fetch', fetcher);
        const confirm = vi.spyOn(window, 'confirm').mockReturnValue(true);
        render(<HooksEditor sessionId="session" onClose={() => {}} />);
        await waitFor(() => expect(screen.getByRole('textbox')).toHaveValue('# original'));
        fireEvent.change(screen.getByRole('textbox'), { target: { value: '# draft' } });
        fireEvent.click(screen.getByRole('button', { name: '保存配置' }));
        expect(await screen.findByRole('alert')).toHaveTextContent('草稿已保留');
        expect(screen.getByRole('textbox')).toHaveValue('# draft');
        expect(screen.queryByText('配置已保存；未执行 Hook。')).not.toBeInTheDocument();
        confirm.mockReturnValue(false);
        fireEvent.click(screen.getByRole('button', { name: '重新加载' }));
        expect(fetcher).toHaveBeenCalledTimes(2);
    });
    it('cancels and ignores a late load when the session changes', async () => {
        let resolve!: (value: Response) => void;
        const fetcher = vi.fn(() => new Promise<Response>(done => { resolve = done; }));
        vi.stubGlobal('fetch', fetcher);
        const close = vi.fn();
        render(<HooksEditor sessionId="session" onClose={close} />);
        act(() => useSessionStore.setState({ sessionId: 'next' }));
        await act(async () => { resolve(response(original)); });
        expect(close).toHaveBeenCalledOnce();
        expect(screen.getByRole('textbox')).toHaveValue('');
        const options = (fetcher.mock.calls[0] as unknown as [string, RequestInit])[1];
        expect(options.signal?.aborted).toBe(true);
    });
});
