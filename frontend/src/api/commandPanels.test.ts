import { beforeEach, describe, expect, it } from 'vitest';
import { openCommandPanel } from './commandPanels';
import { useAppUiStore } from '@/store/appUiStore';
import { useDialogStore } from '@/store/dialogStore';
import { useSessionStore } from '@/store/sessionStore';

describe('native command panel routing', () => {
    beforeEach(() => {
        useSessionStore.setState({ sessionId: 'active' });
        useDialogStore.getState().closeDialog();
        useAppUiStore.setState({ pendingVisualizationTab: null, mobileNavTab: null });
    });
    it('opens existing theme, skill and memory controls without implicit mutations', () => {
        for (const [component, dialog] of [['ThemePicker', 'settings'], ['SkillsManager', 'skills'], ['MemoryManager', 'memory']]) {
            expect(openCommandPanel({ component, sessionId: 'active' })).toBe(true);
            expect(useDialogStore.getState().activeDialog).toBe(dialog);
        }
    });
    it('routes task inspection for both workbench widths and binds export to its source', () => {
        openCommandPanel({ component: 'TaskManager', sessionId: 'active' });
        expect(useAppUiStore.getState()).toMatchObject({ pendingVisualizationTab: 'tasks', mobileNavTab: 'tasks' });
        openCommandPanel({ component: 'ExportDialog', sessionId: 'active', format: 'markdown' });
        expect(useDialogStore.getState()).toMatchObject({ activeDialog: 'export', dialogData: { sessionId: 'active', format: 'markdown' } });
    });
    it('ignores late or unbound panel commands and does not reinterpret unknown JSX', () => {
        expect(openCommandPanel({ component: 'ExportDialog', sessionId: 'previous' })).toBe(true);
        expect(openCommandPanel({ component: 'ThemePicker' })).toBe(true);
        expect(openCommandPanel({ component: 'Unknown' })).toBe(false);
        expect(useDialogStore.getState().activeDialog).toBeNull();
    });
});
