import { cleanup, render, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useSessionStore } from '@/store/sessionStore';
import { FilePreviewDialog } from './FilePreviewDialog';

describe('FilePreviewDialog Markdown images', () => {
    const fetchMock = vi.fn();
    const createObjectUrl = vi.fn(() => 'blob:nested-markdown-image');
    const revokeObjectUrl = vi.fn();

    beforeEach(() => {
        fetchMock.mockReset();
        createObjectUrl.mockClear();
        revokeObjectUrl.mockClear();
        useSessionStore.setState({ sessionId: 'session-1' });
        vi.stubGlobal('fetch', fetchMock);
        Object.defineProperty(URL, 'createObjectURL', { configurable: true, value: createObjectUrl });
        Object.defineProperty(URL, 'revokeObjectURL', { configurable: true, value: revokeObjectUrl });
    });

    afterEach(() => {
        cleanup();
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
        useSessionStore.setState({ sessionId: null });
    });

    it('loads a relative image from the previewed Markdown file directory', async () => {
        fetchMock.mockImplementation(async (input: string) => {
            if (input.includes('docs%2Fguide%2Freadme.md')) {
                return new Response('![diagram](assets/diagram.png)', {
                    headers: { 'content-type': 'text/markdown' },
                });
            }
            return new Response(new Blob(['image'], { type: 'image/png' }), {
                headers: { 'content-type': 'image/png' },
            });
        });

        render(
            <FilePreviewDialog
                sessionId="session-1"
                path="docs/guide/readme.md"
                onClose={vi.fn()}
                onFallback={vi.fn()}
            />,
        );

        await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
        expect(fetchMock.mock.calls[1][0]).toBe(
            '/api/sessions/session-1/files/preview?path=docs%2Fguide%2Fassets%2Fdiagram.png',
        );
    });
});
