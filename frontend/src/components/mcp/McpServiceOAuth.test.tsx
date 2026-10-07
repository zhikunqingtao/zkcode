import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import { McpServiceOAuth } from './McpServiceOAuth';

const response = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

it('shows an explicit consent link and clears the client secret after sending it once', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(response({ state: 'idle' })).mockResolvedValueOnce(response({ authorizationUrl: 'https://auth.example/authorize?state=public', issuer: 'https://auth.example', resource: 'https://mcp.example/mcp', redirectUri: 'http://127.0.0.1:1234/callback', scope: 'read', expiresIn: 300 })).mockImplementation(async () => response({ state: 'pending' }));
    vi.stubGlobal('fetch', fetch);
    render(<McpServiceOAuth name="alpha" enabled />);
    await act(async () => { fireEvent.click(screen.getByText('OAuth 授权设置')); });
    fireEvent.change(screen.getByLabelText('alpha OAuth 客户端 ID'), { target: { value: 'issued-client' } });
    const secret = screen.getByLabelText('alpha OAuth 客户端密钥');
    fireEvent.change(secret, { target: { value: 'form-secret' } });
    await act(async () => { fireEvent.click(screen.getByText('开始授权')); });
    expect(secret).toHaveValue('');
    const link = await screen.findByRole('link', { name: '在浏览器中审核并授权' });
    expect(link).toHaveAttribute('href', 'https://auth.example/authorize?state=public');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    expect(fetch).toHaveBeenCalledWith('/api/mcp/services/alpha/oauth/authorize', expect.objectContaining({ method: 'POST', body: '{"clientId":"issued-client","clientSecret":"form-secret"}' }));
    expect(screen.queryByText('form-secret')).not.toBeInTheDocument();
});

it('does not offer authorization for disabled services or turn a server error into success', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({ state: 'idle' })));
    const view = render(<McpServiceOAuth name="alpha" enabled={false} />);
    await act(async () => { fireEvent.click(screen.getByText('OAuth 授权设置')); });
    expect(screen.getByText('开始授权')).toBeDisabled();
    view.rerender(<McpServiceOAuth name="alpha" enabled />);
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response({ message: 'Keychain unavailable' }, 503)));
    await act(async () => { fireEvent.click(screen.getByText('开始授权')); });
    expect(await screen.findByRole('alert')).toHaveTextContent('Keychain unavailable');
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
});

it('reports unconfirmed remote revocation separately from successful local logout', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(response({ state: 'authorized' })).mockResolvedValueOnce(response({ services: [] })).mockResolvedValueOnce(response({ loggedOut: true, remoteRevoked: false })).mockResolvedValue(response({ services: [] }));
    vi.stubGlobal('fetch', fetch);
    render(<McpServiceOAuth name="alpha" enabled />);
    await act(async () => { fireEvent.click(screen.getByText('OAuth 授权设置')); });
    await waitFor(() => expect(screen.getByRole('status')).toHaveTextContent('授权已保存'));
    await act(async () => { fireEvent.click(screen.getByText('退出授权')); });
    expect(await screen.findByText(/远端撤销未确认/)).toBeInTheDocument();
    expect(screen.getByRole('status')).toHaveTextContent('尚未授权');
});
