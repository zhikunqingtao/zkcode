import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { usePromptAttachments } from './usePromptAttachments';
import { useModelStore, type ModelStoreState } from '@/store/modelStore';
import { useSessionStore } from '@/store/sessionStore';
import { useNotificationStore } from '@/store/notificationStore';
import { usePromptDraftStore } from '@/store/promptDraftStore';

beforeEach(() => {
    useSessionStore.setState({ model: 'm' });
    useModelStore.setState({ models: [], loaded: false, loading: false, error: null });
    useNotificationStore.getState().clearAll();
    usePromptDraftStore.setState({ drafts: {} });
});
afterEach(() => { vi.unstubAllGlobals(); });
describe('image capability availability', () => {
    it('treats absent API capability metadata as unavailable and retries through the model directory', async () => {
        vi.stubGlobal('fetch', vi.fn()
            .mockResolvedValueOnce({ ok: true, json: async () => ({ models: [{ id: 'm', supportsImages: true }] }) })
            .mockResolvedValueOnce({ ok: true, json: async () => ({ models: [{ id: 'm', supportsImages: true, maxImages: 31 }] }) }));
        await useModelStore.getState().fetchModels();
        const { result } = renderHook(() => usePromptAttachments({ runActive: false, compacting: false, sessionId: 's' }));
        expect(result.current.imageCapability).toBe('unavailable');
        await act(() => result.current.retryImageCapabilities());
        expect(result.current.imageCapability).toBe('ready');
        expect(result.current.maxImages).toBe(31);
    });
    it.each([
        [{ loading: true }, 'loading'],
        [{ error: 'offline' }, 'unavailable'],
        [{ loaded: true }, 'unavailable'],
        [{ loaded: true, models: [{ id: 'm', displayName: 'M', supportsImages: false, maxImages: 0 }] }, 'unsupported'],
        [{ loaded: true, models: [{ id: 'm', displayName: 'M', supportsImages: true, maxImages: 3 }] }, 'ready'],
    ] as Array<[Partial<ModelStoreState>, string]>)('distinguishes %j as %s', (state, expected) => {
        useModelStore.setState(state);
        const { result } = renderHook(() => usePromptAttachments({ runActive: false, compacting: false, sessionId: 's' }));
        expect(result.current.imageCapability).toBe(expected);
        if (expected === 'ready') expect(result.current.maxImages).toBe(3);
    });
    it('does not read rejected unknown-capability images or call them zero-limit', async () => {
        const reader = vi.spyOn(FileReader.prototype, 'readAsDataURL');
        useModelStore.setState({ loading: true });
        const { result } = renderHook(() => usePromptAttachments({ runActive: false, compacting: false, sessionId: 's' }));
        await act(() => result.current.handleFiles([new File(['x'], 'a.png', { type: 'image/png' })]));
        expect(reader).not.toHaveBeenCalled();
        expect(useNotificationStore.getState().notifications.at(-1)?.message).toContain('加载');
        expect(useNotificationStore.getState().notifications.at(-1)?.message).not.toContain('0/0');
        reader.mockRestore();
    });
});
