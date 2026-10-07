import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { useMcpServiceStore } from '@/store/mcpServiceStore';
import { McpServicesPanel } from './McpServicesPanel';

const alpha = { name: 'alpha', transport: 'STDIO', scope: 'USER', enabled: true, status: 'failed', toolCount: 0 };
const response = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });
beforeEach(() => useMcpServiceStore.setState({ services: [], loading: false, changing: null, error: null }));
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

it('distinguishes enabled intent from a failed connection and persists the actual switch', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(response({ services: [alpha] })).mockResolvedValueOnce(response({ ...alpha, enabled: false, status: 'disabled' }));
    vi.stubGlobal('fetch', fetch);
    render(<McpServicesPanel />);
    const control = await screen.findByRole('switch', { name: 'alpha 服务' });
    expect(control).toHaveAttribute('aria-checked', 'true');
    expect(screen.getByText(/连接失败/)).toBeInTheDocument();
    expect(screen.getByText(/影响所有项目/)).toBeInTheDocument();
    await act(async () => { fireEvent.click(control); });
    await waitFor(() => expect(control).toHaveAttribute('aria-checked', 'false'));
    expect(fetch).toHaveBeenCalledTimes(2);
});

it('displays persistence errors without claiming that the switch changed', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(response({ services: [alpha] })).mockResolvedValueOnce(response({ message: '配置保存失败' }, 503)).mockResolvedValueOnce(response({ services: [alpha] })));
    render(<McpServicesPanel />);
    const control = await screen.findByRole('switch', { name: 'alpha 服务' });
    await act(async () => { fireEvent.click(control); });
    expect(await screen.findByRole('alert')).toHaveTextContent('配置保存失败');
    expect(control).toHaveAttribute('aria-checked', 'true');
});
