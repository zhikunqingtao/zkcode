import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import PromptInput from './index';
import { useEditorPreferencesStore } from '@/store/editorPreferencesStore';
import { useDialogStore } from '@/store/dialogStore';
import { usePermissionStore } from '@/store/permissionStore';
import { usePromptDraftStore } from '@/store/promptDraftStore';
import { useSessionStore } from '@/store/sessionStore';
import { useAppUiStore } from '@/store/appUiStore';

function composer(runActive = false) {
    const submit = vi.fn().mockResolvedValue(true); const interrupt = vi.fn();
    render(<PromptInput sessionId="editor-test" onSubmit={submit} onSlashCommand={vi.fn().mockResolvedValue(true)} onInterrupt={interrupt} onImmediateInterrupt={interrupt} disabled={false} runActive={runActive} compacting={false} permissionMode="dont_ask" messages={[]} commands={[]} />);
    return { input: screen.getByRole('textbox', { name: '输入消息' }) as HTMLTextAreaElement, submit, interrupt };
}
beforeEach(() => {
    useEditorPreferencesStore.setState({ preferences: { vimEnabled: false, keybindings: {} }, loading: false, saving: false });
    useDialogStore.setState({ activeDialog: null });
    usePermissionStore.setState({ pendingPermissions: [] });
    useAppUiStore.setState({ elicitationDialog: null });
    useSessionStore.setState({ sessionId: 'editor-test' });
    usePromptDraftStore.setState({ drafts: {} });
});
afterEach(cleanup);
it('dispatches the configured submit key and leaves an unmapped Enter to native editing', async () => {
    useEditorPreferencesStore.setState({ preferences: { vimEnabled: false, keybindings: { 'chat:submit': 'ctrl+enter' } } });
    const { input, submit } = composer();
    fireEvent.change(input, { target: { value: 'configured submit' } });
    fireEvent.keyDown(input, { key: 'Enter' }); expect(submit).not.toHaveBeenCalled();
    fireEvent.keyDown(input, { key: 'Enter', ctrlKey: true });
    await waitFor(() => expect(submit).toHaveBeenCalledTimes(1));
    expect(submit.mock.calls[0][0].text).toBe('configured submit');
});
it('does not open global actions over an active modal', () => {
    composer(); act(() => useDialogStore.setState({ activeDialog: 'settings' }));
    fireEvent.keyDown(window, { key: 'k', ctrlKey: true });
    expect(screen.queryByPlaceholderText('Type a command...')).toBeNull();
});
it('keeps textarea selection copy available while Ctrl+C at a caret interrupts immediately', () => {
    const { input, interrupt } = composer(true);
    fireEvent.change(input, { target: { value: 'copy this text' } });
    input.setSelectionRange(0, 4);
    fireEvent.keyDown(input, { key: 'c', ctrlKey: true }); expect(interrupt).not.toHaveBeenCalled();
    input.setSelectionRange(4, 4);
    fireEvent.keyDown(input, { key: 'c', ctrlKey: true }); expect(interrupt).toHaveBeenCalledTimes(1);
});
