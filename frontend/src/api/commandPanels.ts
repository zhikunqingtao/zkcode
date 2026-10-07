import { useAppUiStore } from '@/store/appUiStore';
import { useDialogStore } from '@/store/dialogStore';
import { useSessionStore } from '@/store/sessionStore';

/** Route server-owned UI commands without treating history or text as actions. */
export function openCommandPanel(data: Record<string, unknown>): boolean {
    const component = data.component;
    if (!['ThemePicker', 'SkillsManager', 'TaskManager', 'ExportDialog', 'MemoryManager', 'HooksEditor', 'RewindDialog'].includes(String(component))) return false;
    const sessionId = useSessionStore.getState().sessionId;
    if (typeof data.sessionId !== 'string' || data.sessionId !== sessionId) return true;
    switch (component) {
        case 'ThemePicker': useDialogStore.getState().openDialog('settings'); break;
        case 'SkillsManager': useDialogStore.getState().openDialog('skills'); break;
        case 'MemoryManager': useDialogStore.getState().openDialog('memory'); break;
        case 'HooksEditor': useDialogStore.getState().openDialog('hooks', { sessionId }); break;
        case 'RewindDialog': useDialogStore.getState().openDialog('rewind', { sessionId }); break;
        case 'TaskManager':
            useAppUiStore.getState().requestVisualizationTab('tasks');
            useAppUiStore.getState().setMobileNavTab('tasks');
            break;
        case 'ExportDialog': useDialogStore.getState().openDialog('export', { sessionId, format: data.format === 'markdown' ? 'markdown' : 'json' }); break;
    }
    return true;
}
