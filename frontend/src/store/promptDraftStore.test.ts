import { beforeEach, describe, expect, it } from 'vitest';
import type { LocalAttachment } from '@/types';
import {
    PROMPT_DRAFT_FALLBACK_KEY,
    capturePromptDraftTarget,
    resolvePromptDraftKey,
    usePromptDraftStore,
} from './promptDraftStore';

function makeAttachment(id: string): LocalAttachment {
    return {
        id,
        name: `${id}.png`,
        size: 8,
        type: 'image/png',
        file: new File([new Uint8Array(8)], `${id}.png`, { type: 'image/png' }),
        base64Content: 'AAAA',
        previewUrl: `blob:${id}`,
    };
}


describe('promptDraftStore', () => {
    beforeEach(() => {
        usePromptDraftStore.setState({ drafts: {} });
    });

    it('keeps an operation attached to its draft through migration and later fallback reuse', () => {
        const resolveTarget = capturePromptDraftTarget(PROMPT_DRAFT_FALLBACK_KEY);
        usePromptDraftStore.getState().migrateFallbackTo('session-a', usePromptDraftStore.getState().drafts[PROMPT_DRAFT_FALLBACK_KEY].id);
        usePromptDraftStore.getState().setInput(PROMPT_DRAFT_FALLBACK_KEY, 'new draft');
        usePromptDraftStore.getState().migrateFallbackTo('session-b', usePromptDraftStore.getState().drafts[PROMPT_DRAFT_FALLBACK_KEY].id);
        expect(resolveTarget()).toBe('session-a');
    });

    it('keeps both drafts if a confirmed target already owns another draft', () => {
        usePromptDraftStore.getState().setInput(PROMPT_DRAFT_FALLBACK_KEY, 'home');
        const homeId = usePromptDraftStore.getState().drafts[PROMPT_DRAFT_FALLBACK_KEY].id;
        usePromptDraftStore.getState().setInput('target', 'target text');
        const before = usePromptDraftStore.getState().drafts;
        expect(usePromptDraftStore.getState().migrateFallbackTo('target', homeId)).toBe(false);
        expect(usePromptDraftStore.getState().drafts).toEqual(before);
    });

    it.each([false, true])('repeated transfer of the same identity is an idempotent success (new home: %s)', replacement => {
        usePromptDraftStore.getState().setInput(PROMPT_DRAFT_FALLBACK_KEY, 'transferred text');
        const id = usePromptDraftStore.getState().drafts[PROMPT_DRAFT_FALLBACK_KEY].id;
        expect(usePromptDraftStore.getState().migrateFallbackTo('target', id)).toBe(true);
        if (replacement) usePromptDraftStore.getState().setInput(PROMPT_DRAFT_FALLBACK_KEY, 'new home');
        const before = usePromptDraftStore.getState().drafts;
        expect(usePromptDraftStore.getState().migrateFallbackTo('target', id)).toBe(true);
        expect(usePromptDraftStore.getState().drafts).toEqual(before);
    });

    it('does not move a replacement home draft using an old creation identity', () => {
        const oldId = usePromptDraftStore.getState().ensureDraft(PROMPT_DRAFT_FALLBACK_KEY);
        usePromptDraftStore.getState().clear(PROMPT_DRAFT_FALLBACK_KEY);
        usePromptDraftStore.getState().setInput(PROMPT_DRAFT_FALLBACK_KEY, 'replacement');
        expect(usePromptDraftStore.getState().migrateFallbackTo('target', oldId)).toBe(false);
        expect(usePromptDraftStore.getState().drafts[PROMPT_DRAFT_FALLBACK_KEY].input).toBe('replacement');
        expect(usePromptDraftStore.getState().drafts.target).toBeUndefined();
    });

    it('invalidates an old operation when its draft is cleared and recreated', () => {
        const resolveTarget = capturePromptDraftTarget('session-a');
        usePromptDraftStore.getState().clear('session-a');
        usePromptDraftStore.getState().setInput('session-a', 'replacement draft');
        expect(resolveTarget()).toBeUndefined();
    });

    it('stores and reads the input draft per session', () => {
        usePromptDraftStore.getState().setInput('session-a', 'draft a');

        expect(usePromptDraftStore.getState().drafts['session-a']?.input).toBe('draft a');
        expect(usePromptDraftStore.getState().drafts['session-b']).toBeUndefined();
    });

    it('supports functional updates for the input draft', () => {
        usePromptDraftStore.getState().setInput('session-a', 'hello');
        usePromptDraftStore.getState().setInput('session-a', prev => `${prev} world`);

        expect(usePromptDraftStore.getState().drafts['session-a']?.input).toBe('hello world');
    });

    it('stores attachments per session alongside the input draft', () => {
        const attachment = makeAttachment('att-1');
        usePromptDraftStore.getState().setAttachments('session-a', [attachment]);
        usePromptDraftStore.getState().setInput('session-a', 'with image');

        const draft = usePromptDraftStore.getState().drafts['session-a'];
        expect(draft?.attachments).toHaveLength(1);
        expect(draft?.attachments[0]).toMatchObject({ id: 'att-1', previewUrl: 'blob:att-1' });
        expect(draft?.input).toBe('with image');
    });

    it('supports functional updates for attachments', () => {
        usePromptDraftStore.getState().setAttachments('session-a', [makeAttachment('att-1')]);
        usePromptDraftStore.getState()
            .setAttachments('session-a', prev => [...prev, makeAttachment('att-2')]);
        usePromptDraftStore.getState()
            .setAttachments('session-a', prev => prev.filter(a => a.id !== 'att-1'));

        const attachments = usePromptDraftStore.getState().drafts['session-a']?.attachments;
        expect(attachments?.map(a => a.id)).toEqual(['att-2']);
    });

    it('keeps drafts isolated between two session ids', () => {
        usePromptDraftStore.getState().setInput('session-a', 'draft a');
        usePromptDraftStore.getState().setAttachments('session-a', [makeAttachment('att-a')]);
        usePromptDraftStore.getState().setInput('session-b', 'draft b');
        usePromptDraftStore.getState().setAttachments('session-b', [makeAttachment('att-b')]);

        const { drafts } = usePromptDraftStore.getState();
        expect(drafts['session-a']?.input).toBe('draft a');
        expect(drafts['session-a']?.attachments.map(a => a.id)).toEqual(['att-a']);
        expect(drafts['session-b']?.input).toBe('draft b');
        expect(drafts['session-b']?.attachments.map(a => a.id)).toEqual(['att-b']);
    });

    it('clearing one session does not affect another', () => {
        usePromptDraftStore.getState().setInput('session-a', 'draft a');
        usePromptDraftStore.getState().setAttachments('session-a', [makeAttachment('att-a')]);
        usePromptDraftStore.getState().setInput('session-b', 'draft b');
        usePromptDraftStore.getState().setAttachments('session-b', [makeAttachment('att-b')]);

        usePromptDraftStore.getState().clear('session-a');

        const { drafts } = usePromptDraftStore.getState();
        expect(drafts['session-a']).toBeUndefined();
        expect(drafts['session-b']?.input).toBe('draft b');
        expect(drafts['session-b']?.attachments.map(a => a.id)).toEqual(['att-b']);
    });

    it('clearing an unknown session is a no-op', () => {
        usePromptDraftStore.getState().clear('session-missing');

        expect(usePromptDraftStore.getState().drafts).toEqual({});
    });

    it('resolves a stable fallback key for empty session ids', () => {
        expect(PROMPT_DRAFT_FALLBACK_KEY).toBe('__none__');
        expect(resolvePromptDraftKey(null)).toBe(PROMPT_DRAFT_FALLBACK_KEY);
        expect(resolvePromptDraftKey(undefined)).toBe(PROMPT_DRAFT_FALLBACK_KEY);
        expect(resolvePromptDraftKey('')).toBe(PROMPT_DRAFT_FALLBACK_KEY);
        expect(resolvePromptDraftKey('session-a')).toBe('session-a');
    });

    it('migrates the fallback draft onto the first bound session', () => {
        usePromptDraftStore.getState().setInput(PROMPT_DRAFT_FALLBACK_KEY, 'unsent draft');
        usePromptDraftStore.getState()
            .setAttachments(PROMPT_DRAFT_FALLBACK_KEY, [makeAttachment('att-1')]);

        usePromptDraftStore.getState().migrateFallbackTo('session-new', usePromptDraftStore.getState().drafts[PROMPT_DRAFT_FALLBACK_KEY].id);

        const { drafts } = usePromptDraftStore.getState();
        expect(drafts[PROMPT_DRAFT_FALLBACK_KEY]).toBeUndefined();
        expect(drafts['session-new']?.input).toBe('unsent draft');
        expect(drafts['session-new']?.attachments.map(a => a.id)).toEqual(['att-1']);
    });










});
