import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { Sidebar, SidebarTabContent } from './Sidebar';
import { dispatch } from '@/api/dispatch';
import { sendToServer } from '@/api/stompClient';
import { captureSessionSelectionGuard, clearSessionSelection } from '@/services/sessionActivation';
import { useSessionStore } from '@/store/sessionStore';
import { usePromptDraftStore } from '@/store/promptDraftStore';
import { useNotificationStore } from '@/store/notificationStore';
import { runtimeEnvelope } from '@/test/runtimeEnvelope';

vi.mock('@/api/stompClient', () => ({
    isWsConnected: vi.fn(() => true),
    waitForWsConnection: vi.fn(() => Promise.resolve()),
    sendToServer: vi.fn(() => true),
}));

beforeEach(() => {
    localStorage.clear();
    clearSessionSelection();
    vi.clearAllMocks();
    usePromptDraftStore.setState({ drafts: {} });
    useNotificationStore.getState().clearAll();
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => ({
        ok: true,
        json: async () => String(input).startsWith('/api/sessions?') ? {
            sessions: [{ id: 'existing', title: '已有草稿会话', model: 'test', workingDirectory: '/workspace', updatedAt: new Date().toISOString(), messageCount: 1 }],
            hasMore: false,
        } : [],
    })));
    window.matchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: false, media: query, onchange: null,
        addEventListener: vi.fn(), removeEventListener: vi.fn(),
        addListener: vi.fn(), removeListener: vi.fn(), dispatchEvent: vi.fn(),
    })) as typeof window.matchMedia;
    vi.mocked(sendToServer).mockImplementation((destination, payload) => {
        if (destination === '/app/bind-session') {
            const binding = payload as { sessionId: string; bindRequestId: string; bindingEpoch: number };
            dispatch({
                ...runtimeEnvelope(), type: 'session_restored', protocolVersion: 4,
                bindRequestId: binding.bindRequestId, bindingEpoch: binding.bindingEpoch,
                messages: [],
                metadata: { sessionId: binding.sessionId, model: 'test', permissionMode: 'DEFAULT', status: 'idle' },
            } as never);
        }
        return true;
    });
});

afterEach(() => vi.unstubAllGlobals());

it.each(['desktop', 'mobile'] as const)('%s history navigation binds without transferring home drafts', async navigation => {
    usePromptDraftStore.getState().setInput('__none__', 'home draft');
    usePromptDraftStore.getState().setInput('existing', 'existing draft');
    const drafts = usePromptDraftStore.getState().drafts;
    const creationStillCurrent = captureSessionSelectionGuard();
    if (navigation === 'desktop') render(<Sidebar />);
    else render(<SidebarTabContent activeTab="sessions" onBack={() => {}} />);
    fireEvent.click(await screen.findByText('已有草稿会话'));
    await waitFor(() => expect(useSessionStore.getState().sessionId).toBe('existing'));
    expect(creationStillCurrent()).toBe(false);
    expect(usePromptDraftStore.getState().drafts).toEqual(drafts);
    expect(useNotificationStore.getState().notifications).toEqual([]);
    expect(sendToServer).toHaveBeenCalledWith('/app/bind-session', expect.objectContaining({ sessionId: 'existing' }));
});
