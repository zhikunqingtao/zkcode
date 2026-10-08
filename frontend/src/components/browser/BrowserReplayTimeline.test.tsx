import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, waitFor, fireEvent, act } from '@testing-library/react';
import BrowserReplayTimeline from './BrowserReplayTimeline';

const frame = {
    snapshotId: 'frame-1', sessionId: 'session-1', capturedAt: '2026-10-07T00:00:00Z',
    url: 'https://example.com', title: 'Example', selector: null, nodeCount: 5,
    interactive: [{ role: 'button', name: 'Confirm' }], tree: null, screenshotBase64: null,
};
const response = (data: unknown, status = 200) => ({ ok: status < 400, status, json: async () => data }) as Response;
const props = { open: true, onClose: vi.fn(), sessionId: 'session-1', inline: true };

describe('Browser replay production panel', () => {
    beforeEach(() => { vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response([frame]))); });
    afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });
    it('loads the exact session scope and renders its real frame', async () => {
        render(<BrowserReplayTimeline {...props} />);
        await screen.findByText('Example');
        expect(fetch).toHaveBeenCalledWith('/api/browser/replay/session-1', expect.objectContaining({
            method: 'GET', headers: { 'X-Session-Id': 'session-1' }, signal: expect.any(AbortSignal),
        }));
        fireEvent.click(screen.getByTitle('刷新'));
        await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));
    });
    it('renders the password-safe projection without treating the ARIA replacement as a failure', async () => {
        vi.mocked(fetch).mockResolvedValueOnce(response([{ ...frame, captureStatus: 'complete',
            tree: { source: 'safe_dom_v1', safe_dom: '<input type="password" value="[redacted]">' },
            components: { safe_dom: { status: 'ok' }, interactive: { status: 'ok' }, aria: { status: 'not_requested', reason: 'PASSWORD_SAFE_PROJECTION' }, screenshot: { status: 'not_requested' } },
        }]));
        render(<BrowserReplayTimeline {...props} />);
        fireEvent.click(await screen.findByText('Example'));
        expect(screen.getByText('<input type="password" value="[redacted]">')).toBeInTheDocument();
        expect(screen.queryByRole('alert')).not.toBeInTheDocument();
        expect(document.querySelector('input[type=password]')).toBeNull();
    });
    it('reports partial capture and component failure rather than claiming a complete snapshot', async () => {
        vi.mocked(fetch).mockResolvedValueOnce(response([{ ...frame, captureStatus: 'partial',
            components: { screenshot: { status: 'failed', error_code: 'SCREENSHOT_TIMEOUT' }, safe_dom: { status: 'ok', truncated: true } },
        }]));
        render(<BrowserReplayTimeline {...props} />);
        fireEvent.click(await screen.findByText('Example'));
        expect(screen.getByRole('alert')).toHaveTextContent('SCREENSHOT_TIMEOUT');
        expect(screen.getByRole('alert')).toHaveTextContent('截断');
    });
    it('requires ordinary confirmation and server acknowledgement before clearing', async () => {
        const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false);
        render(<BrowserReplayTimeline {...props} />);
        await screen.findByText('Example');
        fireEvent.click(screen.getByTitle('清空'));
        expect(fetch).toHaveBeenCalledTimes(1);
        confirm.mockReturnValue(true);
        vi.mocked(fetch).mockResolvedValueOnce(response({ code: 'STORE_FAILED' }, 500));
        fireEvent.click(screen.getByTitle('清空'));
        await screen.findByText(/HTTP 500/);
        expect(screen.getByText('Example')).toBeInTheDocument();
        vi.mocked(fetch).mockResolvedValueOnce(response({ status: 'deleted', replayId: 'session-1' }));
        fireEvent.click(screen.getByTitle('清空'));
        await screen.findByText(/暂无快照/);
        expect(fetch).toHaveBeenLastCalledWith('/api/browser/replay/session-1', expect.objectContaining({ method: 'DELETE', headers: { 'X-Session-Id': 'session-1' } }));
    });
    it('expands real interaction detail and displays a stored screenshot', async () => {
        vi.mocked(fetch).mockResolvedValueOnce(response([{ ...frame, screenshotBase64: 'iVBORw0KGgo=' }]));
        render(<BrowserReplayTimeline {...props} />);
        fireEvent.click(await screen.findByText('Example'));
        expect(screen.getByText('Confirm')).toBeInTheDocument();
        expect(screen.getByRole('img')).toHaveAttribute('src', 'data:image/png;base64,iVBORw0KGgo=');
    });
    it('distinguishes absent replay from failed authorization and temporary evidence', async () => {
        vi.mocked(fetch).mockResolvedValueOnce(response({ code: 'REPLAY_NOT_FOUND' }, 404));
        render(<BrowserReplayTimeline {...props} />);
        await screen.findByText(/暂无快照/);
        vi.mocked(fetch).mockResolvedValueOnce(response({ code: 'EPHEMERAL_OPERATION_UNSUPPORTED' }, 400));
        fireEvent.click(screen.getByTitle('刷新'));
        await screen.findByText(/临时会话不保存磁盘时间线/);
        vi.mocked(fetch).mockResolvedValueOnce(response({ code: 'SESSION_NOT_FOUND' }, 404));
        fireEvent.click(screen.getByTitle('刷新'));
        await screen.findByText(/HTTP 404/);
    });
    it('rejects cross-session frames without rendering their body', async () => {
        vi.mocked(fetch).mockResolvedValueOnce(response([{ ...frame, sessionId: 'foreign', title: 'private foreign title' }]));
        render(<BrowserReplayTimeline {...props} />);
        await screen.findByText(/无效或跨会话数据/);
        expect(screen.queryByText('private foreign title')).not.toBeInTheDocument();
    });
    it('aborts old requests on session change and close, even when transport resolves late', async () => {
        let complete!: (value: Response) => void;
        vi.mocked(fetch).mockImplementationOnce(() => new Promise(resolve => { complete = resolve; }));
        const { rerender } = render(<BrowserReplayTimeline {...props} />);
        const firstSignal = vi.mocked(fetch).mock.calls[0][1]?.signal;
        vi.mocked(fetch).mockResolvedValueOnce(response([{ ...frame, sessionId: 'session-2', title: 'Current' }]));
        rerender(<BrowserReplayTimeline {...props} sessionId="session-2" />);
        await screen.findByText('Current');
        expect(firstSignal?.aborted).toBe(true);
        await act(async () => { complete(response([frame])); });
        expect(screen.queryByText('Example')).not.toBeInTheDocument();
        expect(screen.getByText('Current')).toBeInTheDocument();
        const currentSignal = vi.mocked(fetch).mock.calls[1][1]?.signal;
        rerender(<BrowserReplayTimeline {...props} open={false} sessionId="session-2" />);
        expect(currentSignal?.aborted).toBe(true);
    });
});
