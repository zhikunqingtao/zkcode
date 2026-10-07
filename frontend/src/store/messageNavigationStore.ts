import { create } from 'zustand';

interface MessageNavigationState {
    activeSessionId: string | null;
    pendingMessageId: string | null;
    setActiveSession: (sessionId: string | null) => void;
    openMessage: (messageId: string) => void;
    consumePendingMessage: () => void;
}

/** Message deep links are scoped to the selected session, independent of display density. */
export const useMessageNavigationStore = create<MessageNavigationState>((set) => ({
    activeSessionId: null,
    pendingMessageId: null,
    setActiveSession: (sessionId) => set(state => {
        if (state.activeSessionId === sessionId) return state;
        return { activeSessionId: sessionId, pendingMessageId: null };
    }),
    openMessage: (messageId) => set({ pendingMessageId: messageId }),
    consumePendingMessage: () => set({ pendingMessageId: null }),
}));
