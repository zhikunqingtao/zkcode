import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SettingsPanel } from '@/components/dialog/SettingsPanel';
import { useNotificationStore } from '@/store/notificationStore';
import { usePermissionStore } from '@/store/permissionStore';
import { useSessionStore } from '@/store/sessionStore';

const { binding, sendSetPermissionMode } = vi.hoisted(() => ({
    binding: { bound: true },
    sendSetPermissionMode: vi.fn(() => true),
}));

vi.mock('@/api/dispatch', () => ({
    isSessionBound: () => binding.bound,
}));

vi.mock('@/api/stompClient', () => ({
    sendSetPermissionMode,
}));

describe('SettingsPanel permission modes', () => {
    beforeEach(() => {
        sendSetPermissionMode.mockClear();
        sendSetPermissionMode.mockReturnValue(true);
        binding.bound = true;
        useSessionStore.setState({ sessionId: 'session-1' });
        usePermissionStore.setState({ permissionMode: 'default', pendingPermissions: [] });
        useNotificationStore.getState().clearAll();
    });

    afterEach(() => {
        vi.unstubAllGlobals();
    });

    it('mounts the API key manager through the production settings dialog', async () => {
        const fetchMock = vi.fn().mockResolvedValue(jsonResponse({
            providers: [{
                name: 'dashscope-token-plan',
                label: 'DashScope Token Plan',
                has_key: true,
                masked_key: 'demo…key',
            }],
        }));
        vi.stubGlobal('fetch', fetchMock);
        render(<SettingsPanel onClose={vi.fn()} />);

        fireEvent.click(screen.getByRole('tab', { name: 'API Keys' }));

        expect(await screen.findByLabelText('DashScope Token Plan API 密钥')).toBeInTheDocument();
        await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
            '/api/llm-keys',
            expect.objectContaining({ signal: expect.any(AbortSignal) }),
        ));
        expect(screen.getByRole('tabpanel', { name: 'API Keys' })).toBeInTheDocument();
    });

    it('shows all five permission modes', () => {
        render(<SettingsPanel onClose={vi.fn()} />);

        expect(screen.getByText('默认模式')).toBeInTheDocument();
        expect(screen.getByText('计划模式')).toBeInTheDocument();
        expect(screen.getByText('接受编辑')).toBeInTheDocument();
        expect(screen.getByText('无需询问')).toBeInTheDocument();
        expect(screen.getByText('完全访问权限')).toBeInTheDocument();
    });

    it('requests AUTO_APPROVE without optimistically changing local state', () => {
        render(<SettingsPanel onClose={vi.fn()} />);

        fireEvent.click(screen.getByText('完全访问权限'));

        expect(sendSetPermissionMode).toHaveBeenCalledWith('AUTO_APPROVE');
        expect(usePermissionStore.getState().permissionMode).toBe('default');
    });

    it('keeps the confirmed mode and reports a transport send failure', () => {
        sendSetPermissionMode.mockReturnValue(false);
        render(<SettingsPanel onClose={vi.fn()} />);

        fireEvent.click(screen.getByText('完全访问权限'));

        expect(usePermissionStore.getState().permissionMode).toBe('default');
        expect(useNotificationStore.getState().notifications)
            .toEqual(expect.arrayContaining([expect.objectContaining({
                key: 'permission-mode-send-failed',
                level: 'error',
            })]));
    });

    it('disables permission changes until the session is bound', () => {
        binding.bound = false;
        render(<SettingsPanel onClose={vi.fn()} />);

        const option = screen.getByText('完全访问权限').closest('button');
        expect(option).toBeDisabled();
        if (option) fireEvent.click(option);
        expect(sendSetPermissionMode).not.toHaveBeenCalled();
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
