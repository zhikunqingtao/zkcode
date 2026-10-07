import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { FileChangesDashboard } from './FileChangesDashboard';
vi.mock('@/hooks/useResponsive', () => ({ useResponsive: () => ({ isMobile: false }) }));
const response = (payload: unknown, status = 200) => ({ ok: status === 200, status, json: async () => payload }) as Response;
const snapshots = { 'a&1': [{ messageId: 'a&1', trackedFiles: ['/repo/a.ts'], timestamp: '2026-01-01' }], 'b 2': [{ messageId: 'b 2', trackedFiles: ['/repo/a.ts'], timestamp: '2026-01-02' }] };
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
it('compares real selected checkpoints with the current session identity only after a click', async () => {
    const fetchMock = vi.fn().mockResolvedValueOnce(response(snapshots)).mockResolvedValueOnce(response({ filesAdded: 0, filesModified: 1, filesDeleted: 0, changedFiles: ['/repo/a.ts'] }));
    vi.stubGlobal('fetch', fetchMock); render(<FileChangesDashboard sessionId="session-A" />);
    await screen.findByRole('button', { name: '比较检查点' });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][1].headers).toEqual({ 'X-Session-Id': 'session-A' });
    fireEvent.click(screen.getByRole('button', { name: '比较检查点' }));
    await screen.findByText('新增 0 · 修改 1 · 删除 0');
    expect(fetchMock.mock.calls[1][0]).toBe('/api/sessions/session-A/history/diff?fromMessageId=a%261&toMessageId=b+2');
    expect(fetchMock.mock.calls[1][1].headers).toEqual({ 'X-Session-Id': 'session-A' });
    expect(screen.getByText(/不代表当前工作区或 Git 变更/)).toBeInTheDocument();
});
it('aborts old comparisons and cannot reveal their late files after switching sessions', async () => {
    let finish!: (value: Response) => void; const pending = new Promise<Response>(done => { finish = done; });
    const fetchMock = vi.fn().mockResolvedValueOnce(response(snapshots)).mockImplementationOnce(() => pending).mockResolvedValueOnce(response({}));
    vi.stubGlobal('fetch', fetchMock); const view = render(<FileChangesDashboard sessionId="A" />);
    fireEvent.click(await screen.findByRole('button', { name: '比较检查点' }));
    const signal = fetchMock.mock.calls[1][1].signal as AbortSignal;
    view.rerender(<FileChangesDashboard sessionId="B" />); expect(signal.aborted).toBe(true);
    await screen.findByText('至少需要两个文件检查点才能比较。');
    await act(async () => { finish(response({ filesAdded: 1, filesModified: 0, filesDeleted: 0, changedFiles: ['/repo/private'] })); });
    expect(screen.queryByText('/repo/private')).not.toBeInTheDocument();
});
it('keeps errors visible instead of interpreting failed responses as no file changes', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({}, 403)));
    render(<FileChangesDashboard sessionId="A" />);
    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('HTTP 403'));
});
