import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ApiKeysTab } from './ApiKeysTab';
import { useNotificationStore } from '@/store/notificationStore';

const provider = {
  name: 'dashscope-token-plan',
  label: 'DashScope Token Plan',
  has_key: true,
  masked_key: 'demo…key',
};

describe('ApiKeysTab', () => {
  beforeEach(() => {
    useNotificationStore.getState().clearAll();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('loads providers and saves only the edited key', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({ providers: [provider] }))
      .mockResolvedValueOnce(jsonResponse({
        providers: [{ ...provider, masked_key: 'new…key' }],
      }));
    vi.stubGlobal('fetch', fetchMock);
    render(<ApiKeysTab />);

    const input = await screen.findByLabelText('DashScope Token Plan API 密钥');
    fireEvent.change(input, { target: { value: 'new-test-key' } });
    fireEvent.click(screen.getByRole('button', { name: '保存' }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(fetchMock).toHaveBeenNthCalledWith(2, '/api/llm-keys', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ keys: { 'dashscope-token-plan': 'new-test-key' } }),
    });
    expect(useNotificationStore.getState().notifications).toEqual(
      expect.arrayContaining([expect.objectContaining({
        key: 'apikeys-save-success',
        level: 'success',
      })]),
    );
  });

  it('reports a structured backend error when clearing a key fails', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({ providers: [provider] }))
      .mockResolvedValueOnce(jsonResponse({
        code: 'LLM_KEY_UPDATE_FAILED',
        message: '无法更新密钥',
        requestId: 'request-123',
      }, 500));
    vi.stubGlobal('fetch', fetchMock);
    render(<ApiKeysTab />);

    await screen.findByLabelText('DashScope Token Plan API 密钥');
    fireEvent.click(screen.getByRole('button', { name: '清除 DashScope Token Plan API 密钥' }));
    fireEvent.click(screen.getByRole('button', { name: '保存' }));

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'LLM_KEY_UPDATE_FAILED：无法更新密钥（请求 ID：request-123）',
    );
    expect(fetchMock).toHaveBeenNthCalledWith(2, '/api/llm-keys', expect.objectContaining({
      body: JSON.stringify({ keys: { 'dashscope-token-plan': '' } }),
    }));
    expect(screen.getByText('将清除已保存密钥')).toBeInTheDocument();
  });

  it('retries a failed provider load without reloading the application', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({
        code: 'TEMPORARY_FAILURE',
        message: '服务暂不可用',
      }, 503))
      .mockResolvedValueOnce(jsonResponse({ providers: [provider] }));
    vi.stubGlobal('fetch', fetchMock);
    render(<ApiKeysTab />);

    expect(await screen.findByRole('alert')).toHaveTextContent('TEMPORARY_FAILURE：服务暂不可用');
    fireEvent.click(screen.getByRole('button', { name: '重试' }));

    expect(await screen.findByLabelText('DashScope Token Plan API 密钥')).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});

function jsonResponse(body: unknown, status = 200): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: new Headers({ 'Content-Type': 'application/json' }),
    json: async () => body,
  } as Response;
}
