import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SessionExportDialog } from './SessionExportDialog';
import { useSessionStore } from '@/store/sessionStore';

vi.mock('@/components/ui', () => ({
    Dialog: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
    Button: ({ children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) => <button {...props}>{children}</button>,
}));
describe('SessionExportDialog', () => {
    beforeEach(() => { useSessionStore.setState({ sessionId: 'session-1' }); });
    afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });
    it('only downloads on explicit click through the bound native endpoint', async () => {
        const fetch = vi.fn().mockResolvedValue(new Response('body', { status: 200 }));
        vi.stubGlobal('fetch', fetch);
        const create = vi.fn(() => 'blob:download');
        const revoke = vi.fn();
        vi.stubGlobal('URL', { createObjectURL: create, revokeObjectURL: revoke });
        const click = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
        render(<SessionExportDialog sessionId="session-1" initialFormat="markdown" onClose={() => {}} />);
        expect(fetch).not.toHaveBeenCalled();
        fireEvent.click(screen.getByRole('button', { name: '下载' }));
        await waitFor(() => expect(click).toHaveBeenCalledOnce());
        expect(fetch).toHaveBeenCalledWith('/api/sessions/session-1/export?format=markdown', expect.objectContaining({ method: 'POST', headers: { 'X-Session-Id': 'session-1' } }));
        await waitFor(() => expect(revoke).toHaveBeenCalledWith('blob:download'));
    });
    it('shows retention refusal without downloading or claiming success', async () => {
        vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({ code: 'EPHEMERAL_OPERATION_UNSUPPORTED' }), { status: 400 })));
        const click = vi.spyOn(HTMLAnchorElement.prototype, 'click');
        render(<SessionExportDialog sessionId="session-1" onClose={() => {}} />);
        fireEvent.click(screen.getByRole('button', { name: '下载' }));
        expect(await screen.findByRole('alert')).toHaveTextContent('临时会话不能导出');
        expect(click).not.toHaveBeenCalled();
    });
    it('aborts on exit and rejects a late result after switching sessions', async () => {
        let resolve!: (response: Response) => void;
        const fetch = vi.fn(() => new Promise<Response>(done => { resolve = done; }));
        vi.stubGlobal('fetch', fetch);
        const click = vi.spyOn(HTMLAnchorElement.prototype, 'click');
        const { unmount } = render(<SessionExportDialog sessionId="session-1" onClose={() => {}} />);
        fireEvent.click(screen.getByRole('button', { name: '下载' }));
        const signal = (fetch.mock.calls[0] as unknown as [string, RequestInit])[1].signal;
        unmount();
        useSessionStore.setState({ sessionId: 'next' });
        resolve(new Response('private'));
        await waitFor(() => expect(signal?.aborted).toBe(true));
        expect(click).not.toHaveBeenCalled();
    });
});
