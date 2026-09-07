import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SettingsPanel } from '@/components/dialog/SettingsPanel';
import { useNotificationStore } from '@/store/notificationStore';
import { usePermissionStore } from '@/store/permissionStore';
import { useSessionStore } from '@/store/sessionStore';
import { useModelStore } from '@/store/modelStore';
import { useConfigStore } from '@/store/configStore';

const { binding, sendSetModel, sendSetPermissionMode } = vi.hoisted(() => ({
    binding: { bound: true },
    sendSetModel: vi.fn(),
    sendSetPermissionMode: vi.fn(() => true),
}));

const originalSaveConfig = useConfigStore.getState().saveConfig;
const saveConfig = vi.fn(async () => {});

vi.mock('@/api/dispatch', () => ({
    isSessionBound: () => binding.bound,
}));

vi.mock('@/api/stompClient', () => ({
    sendSetModel,
    sendSetPermissionMode,
}));

describe('SettingsPanel permission modes', () => {
    beforeEach(() => {
        saveConfig.mockClear();
        sendSetModel.mockClear();
        sendSetPermissionMode.mockClear();
        sendSetPermissionMode.mockReturnValue(true);
        binding.bound = true;
        useConfigStore.setState({
            defaultModel: 'current-model',
            saveConfig,
        });
        useSessionStore.setState({ sessionId: 'session-1', model: 'current-model' });
        useModelStore.setState({
            models: [{
                id: 'current-model',
                displayName: 'Current Model',
                supportsImages: false,
                maxImages: 0,
            }],
            defaultModel: 'current-model',
            loaded: true,
            loading: false,
            error: null,
        });
        usePermissionStore.setState({ permissionMode: 'default', pendingPermissions: [] });
        useNotificationStore.getState().clearAll();
    });

    afterEach(() => {
        useConfigStore.setState({ saveConfig: originalSaveConfig });
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

    it('applies an advertised model to the current and future sessions', () => {
        useModelStore.setState({
            models: [
                {
                    id: 'current-model',
                    displayName: 'Current Model',
                    supportsImages: false,
                    maxImages: 0,
                },
                {
                    id: 'new-model',
                    displayName: 'New Provider Model',
                    supportsImages: true,
                    maxImages: 2,
                },
            ],
            loaded: true,
        });

        render(<SettingsPanel onClose={vi.fn()} />);

        const modelOption = screen.getByRole('option', { name: 'New Provider Model' });
        expect(modelOption).toBeInTheDocument();
        const modelSelect = modelOption.closest('select');
        expect(modelSelect).not.toBeNull();
        expect(Array.from(modelSelect?.options ?? [], (option) => option.value))
            .toEqual(['current-model', 'new-model']);
        if (modelSelect) fireEvent.change(modelSelect, { target: { value: 'new-model' } });
        expect(useSessionStore.getState().model).toBe('new-model');
        expect(saveConfig).toHaveBeenCalledWith({ defaultModel: 'new-model' });
        expect(sendSetModel).toHaveBeenCalledWith('new-model');
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
