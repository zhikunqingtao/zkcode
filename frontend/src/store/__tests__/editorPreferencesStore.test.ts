import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { useEditorPreferencesStore } from '../editorPreferencesStore';
import { DEFAULT_EDITOR_PREFERENCES } from '@/keyboard/shortcuts';
const reply = (value: unknown, status = 200) => new Response(JSON.stringify(value), { status });
beforeEach(() => { useEditorPreferencesStore.setState({ preferences: DEFAULT_EDITOR_PREFERENCES, loaded: false, loading: false, saving: false, error: null }); });
afterEach(() => vi.unstubAllGlobals());
it('saves to the Rust config API and never activates an unconfirmed preference', async () => {
    const preferences = { vimEnabled: true, keybindings: { 'chat:commandPalette': 'ctrl+k ctrl+g' } };
    const fetch = vi.fn().mockResolvedValueOnce(reply({ success: true, config: { editorPreferences: preferences } })).mockResolvedValueOnce(reply({}, 500)); vi.stubGlobal('fetch', fetch);
    expect(await useEditorPreferencesStore.getState().save(preferences)).toBe(true);
    expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual({ editorPreferences: preferences });
    expect(await useEditorPreferencesStore.getState().save(DEFAULT_EDITOR_PREFERENCES)).toBe(false);
    expect(useEditorPreferencesStore.getState().preferences).toEqual(preferences);
    expect(useEditorPreferencesStore.getState().error).toContain('500');
});
it('keeps a valid configuration when the server document becomes malformed', async () => {
    useEditorPreferencesStore.getState().accept({ vimEnabled: true, keybindings: {} });
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(reply({ editorPreferences: { vimEnabled: 'invalid' } })));
    await useEditorPreferencesStore.getState().load();
    expect(useEditorPreferencesStore.getState().preferences.vimEnabled).toBe(true);
    expect(useEditorPreferencesStore.getState().error).toBeTruthy();
});
