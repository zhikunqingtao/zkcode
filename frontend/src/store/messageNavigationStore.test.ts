import { beforeEach, describe, expect, it } from 'vitest';
import { useMessageNavigationStore } from './messageNavigationStore';

describe('messageNavigationStore session boundary', () => {
    beforeEach(() => {
        localStorage.clear();
        useMessageNavigationStore.setState({ activeSessionId: null, pendingMessageId: null });
    });

    it('opens an exact message and consumes the target once', () => {
        useMessageNavigationStore.getState().setActiveSession('session-a');
        useMessageNavigationStore.getState().openMessage('message-42');
        expect(useMessageNavigationStore.getState().pendingMessageId).toBe('message-42');
        useMessageNavigationStore.getState().consumePendingMessage();
        expect(useMessageNavigationStore.getState().pendingMessageId).toBeNull();
    });

    it('retains a target on the same session and clears it before changing sessions', () => {
        const navigation = useMessageNavigationStore.getState();
        navigation.setActiveSession('session-a');
        navigation.openMessage('message-a');
        navigation.setActiveSession('session-a');
        expect(useMessageNavigationStore.getState().pendingMessageId).toBe('message-a');
        navigation.setActiveSession('session-b');
        expect(useMessageNavigationStore.getState()).toMatchObject({ activeSessionId: 'session-b', pendingMessageId: null });
        navigation.openMessage('message-b');
        navigation.setActiveSession(null);
        expect(useMessageNavigationStore.getState().pendingMessageId).toBeNull();
    });

    it('does not rewrite unrelated preferences or legacy workbench storage', () => {
        localStorage.setItem('zhikun.workbench.default-view', 'simple');
        localStorage.setItem('zhikun.workbench.session-view.session-a', 'simple');
        const preference = JSON.stringify({ state: { density: 'detailed', expandOverrides: {} }, version: 2 });
        localStorage.setItem('zhikun.turn-view.v1', preference);
        useMessageNavigationStore.getState().setActiveSession('session-a');
        useMessageNavigationStore.getState().openMessage('message-a');
        expect(localStorage.getItem('zhikun.turn-view.v1')).toBe(preference);
        expect(localStorage.getItem('zhikun.workbench.default-view')).toBe('simple');
        expect(localStorage.getItem('zhikun.workbench.session-view.session-a')).toBe('simple');
    });
});
