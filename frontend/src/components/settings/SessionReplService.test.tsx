import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SessionReplService } from './SessionReplService';
import { useSessionStore } from '@/store/sessionStore';

const status = (state: string, cleanupStatus: string, sessionId = 'session-a') => ({ sessionId, state, cleanupStatus, idleTimeoutSeconds: 600, maxLifetimeSeconds: 3600 });
const response = (value: unknown, code = 200) => new Response(JSON.stringify(value), { status: code, headers: { 'Content-Type': 'application/json' } });
beforeEach(() => useSessionStore.setState({ sessionId: 'session-a' }));
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

describe('SessionReplService', () => {
    it('shows absent as not started and sends current session authorization', async () => {
        const fetcher = vi.fn().mockResolvedValue(response(status('absent', 'notRequired')));
        vi.stubGlobal('fetch', fetcher);
        render(<SessionReplService />);
        expect(await screen.findByText('未启动')).toBeVisible();
        expect(screen.queryByRole('button', { name: '停止 REPL 服务' })).not.toBeInTheDocument();
        expect(fetcher).toHaveBeenCalledWith('/api/sessions/session-a/repl-service', expect.objectContaining({ headers: { 'X-Session-Id': 'session-a' }, signal: expect.any(AbortSignal) }));
    });
    it('requires confirmation and shows pending until the actual cleanup is confirmed', async () => {
        let value = status('running', 'pending');
        const fetcher = vi.fn().mockImplementation((_url: string, options: RequestInit) => {
            if (options.method === 'DELETE') value = status('stopping', 'pending');
            return Promise.resolve(response(value, options.method === 'DELETE' ? 202 : 200));
        });
        vi.stubGlobal('fetch', fetcher);
        render(<SessionReplService />);
        fireEvent.click(await screen.findByRole('button', { name: '停止 REPL 服务' }));
        expect(fetcher.mock.calls.some(([, options]) => options.method === 'DELETE')).toBe(false);
        fireEvent.click(screen.getByRole('button', { name: '确认停止 REPL' }));
        expect(await screen.findByText('正在停止，等待清理完成')).toBeVisible();
        expect(screen.queryByText('已停止')).not.toBeInTheDocument();
        value = status('stopped', 'confirmed');
        fireEvent.click(screen.getByRole('button', { name: '刷新状态' }));
        expect(await screen.findByText('已停止')).toBeVisible();
        expect(fetcher.mock.calls.filter(([, options]) => options.method === 'DELETE')).toHaveLength(1);
    });
    it('never presents unconfirmed cleanup as stopped', async () => {
        vi.stubGlobal('fetch', vi.fn().mockImplementation(() => Promise.resolve(response(status('cleanupUnconfirmed', 'unconfirmed')))));
        render(<SessionReplService />);
        expect(await screen.findByText('清理未确认，请检查后重试')).toBeVisible();
        expect(screen.queryByText('已停止')).not.toBeInTheDocument();
    });
    it('discards a late response from the previous session', async () => {
        let resolveOld!: (value: Response) => void;
        vi.stubGlobal('fetch', vi.fn().mockImplementation((url: string) => url.includes('session-a') ? new Promise<Response>(resolve => { resolveOld = resolve; }) : Promise.resolve(response(status('absent', 'notRequired', 'session-b')))));
        render(<SessionReplService />);
        act(() => useSessionStore.setState({ sessionId: 'session-b' }));
        expect(await screen.findByText('未启动')).toBeVisible();
        await act(async () => { resolveOld(response(status('running', 'pending'))); });
        expect(screen.getByText('未启动')).toBeVisible();
        expect(screen.queryByRole('button', { name: '停止 REPL 服务' })).not.toBeInTheDocument();
    });
    it('retains a stop failure instead of turning a subsequent GET into success', async () => {
        vi.stubGlobal('fetch', vi.fn().mockImplementation((_url: string, options: RequestInit) => Promise.resolve(options.method === 'DELETE' ? response({}, 503) : response(status('running', 'pending')))));
        render(<SessionReplService />);
        fireEvent.click(await screen.findByRole('button', { name: '停止 REPL 服务' }));
        fireEvent.click(screen.getByRole('button', { name: '确认停止 REPL' }));
        await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('HTTP 503'));
        expect(screen.queryByText('已停止')).not.toBeInTheDocument();
    });
});
