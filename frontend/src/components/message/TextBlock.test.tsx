import { act, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useSessionStore } from '@/store/sessionStore';
import TextBlock from './TextBlock';

describe('TextBlock markdown images', () => {
    const fetchMock = vi.fn();
    const createObjectUrl = vi.fn();
    const revokeObjectUrl = vi.fn();

    beforeEach(() => {
        fetchMock.mockReset();
        createObjectUrl.mockReset();
        revokeObjectUrl.mockReset();
        useSessionStore.setState({ sessionId: 'session-1' });
        vi.stubGlobal('fetch', fetchMock);
        Object.defineProperty(URL, 'createObjectURL', { configurable: true, value: createObjectUrl });
        Object.defineProperty(URL, 'revokeObjectURL', { configurable: true, value: revokeObjectUrl });
    });

    afterEach(() => {
        vi.unstubAllGlobals();
        vi.restoreAllMocks();
        useSessionStore.setState({ sessionId: null });
    });

    it('renders GFM tables and direct browser images without invalid paragraph nesting', () => {
        render(<TextBlock text={'| A | B |\n| - | - |\n| 1 | 2 |\n\n![remote](https://images.example.test/a.png)'} />);

        expect(screen.getByRole('table')).toBeInTheDocument();
        const image = screen.getByAltText('remote');
        expect(image).toHaveAttribute('src', 'https://images.example.test/a.png');
        expect(image).toHaveAttribute('referrerpolicy', 'no-referrer');
        expect(image.closest('p')).toBeNull();
        expect(fetchMock).not.toHaveBeenCalled();
    });

    it('loads absolute, relative, and file URLs through the session preview endpoint and cleans them up', async () => {
        let objectUrlIndex = 0;
        createObjectUrl.mockImplementation(() => `blob:preview-${++objectUrlIndex}`);
        fetchMock.mockResolvedValue({
            ok: true,
            blob: async () => new Blob(['image'], { type: 'image/png' }),
        });

        const { unmount } = render(
            <TextBlock text={'![file](file:///workspace/%E5%9B%BE.png)\n\n![absolute](/workspace/b.png)\n\n![relative](images/c.png)'} />,
        );

        await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
        expect(fetchMock.mock.calls.map(call => call[0])).toEqual([
            '/api/sessions/session-1/files/preview?path=%2Fworkspace%2F%E5%9B%BE.png',
            '/api/sessions/session-1/files/preview?path=%2Fworkspace%2Fb.png',
            '/api/sessions/session-1/files/preview?path=images%2Fc.png',
        ]);
        for (const [, options] of fetchMock.mock.calls) {
            expect(options.headers).toEqual({ 'X-Session-Id': 'session-1' });
        }
        await screen.findByAltText('relative');

        const signals = fetchMock.mock.calls.map(([, options]) => options.signal as AbortSignal);
        unmount();
        expect(signals.every(signal => signal.aborted)).toBe(true);
        expect(revokeObjectUrl).toHaveBeenCalledTimes(3);
    });

    it('resolves relative images from the directory of a nested Markdown file', async () => {
        createObjectUrl.mockImplementation(() => 'blob:nested-preview');
        fetchMock.mockResolvedValue({
            ok: true,
            blob: async () => new Blob(['image'], { type: 'image/png' }),
        });

        render(
            <TextBlock
                text={'![child](images/child.png)\n\n![parent](../shared/parent.png)\n\n![absolute](/workspace/root.png)'}
                sourcePath="/workspace/docs/guide/readme.md"
            />,
        );

        await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
        expect(fetchMock.mock.calls.map(call => call[0])).toEqual([
            '/api/sessions/session-1/files/preview?path=%2Fworkspace%2Fdocs%2Fguide%2Fimages%2Fchild.png',
            '/api/sessions/session-1/files/preview?path=%2Fworkspace%2Fdocs%2Fguide%2F..%2Fshared%2Fparent.png',
            '/api/sessions/session-1/files/preview?path=%2Fworkspace%2Froot.png',
        ]);
    });

    it('resolves relative images from Windows-style Markdown paths', async () => {
        createObjectUrl.mockImplementation(() => 'blob:windows-preview');
        fetchMock.mockResolvedValue({
            ok: true,
            blob: async () => new Blob(['image'], { type: 'image/png' }),
        });

        render(<TextBlock text="![child](images/child.png)" sourcePath={'C:\\workspace\\docs\\readme.md'} />);

        await waitFor(() => expect(fetchMock).toHaveBeenCalledOnce());
        expect(fetchMock.mock.calls[0][0]).toBe(
            '/api/sessions/session-1/files/preview?path=C%3A%2Fworkspace%2Fdocs%2Fimages%2Fchild.png',
        );
    });

    it('does not reuse a local image Blob URL after the session changes', async () => {
        let resolveSecond!: (response: { ok: boolean; blob: () => Promise<Blob> }) => void;
        const secondResponse = new Promise<{ ok: boolean; blob: () => Promise<Blob> }>(resolve => {
            resolveSecond = resolve;
        });
        createObjectUrl
            .mockReturnValueOnce('blob:session-1')
            .mockReturnValueOnce('blob:session-2');
        fetchMock
            .mockResolvedValueOnce({
                ok: true,
                blob: async () => new Blob(['first'], { type: 'image/png' }),
            })
            .mockReturnValueOnce(secondResponse);

        render(<TextBlock text="![local](image.png)" />);
        expect(await screen.findByAltText('local')).toHaveAttribute('src', 'blob:session-1');

        act(() => useSessionStore.setState({ sessionId: 'session-2' }));
        expect(screen.queryByAltText('local')).not.toBeInTheDocument();

        resolveSecond({
            ok: true,
            blob: async () => new Blob(['second'], { type: 'image/png' }),
        });
        expect(await screen.findByAltText('local')).toHaveAttribute('src', 'blob:session-2');
    });

    it('allows blob and raster base64 images while rejecting executable or SVG data protocols', () => {
        render(
            <TextBlock text={'![blob](blob:https://app.example/id)\n\n![png](data:image/png;base64,iVBORw0KGgo=)\n\n![script](javascript:alert(1))\n\n![mail](mailto:image@example.test)\n\n![svg](data:image/svg+xml;base64,PHN2Zz4=)'} />,
        );

        expect(screen.getByAltText('blob')).toBeInTheDocument();
        expect(screen.getByAltText('png')).toBeInTheDocument();
        expect(screen.queryByAltText('script')).not.toBeInTheDocument();
        expect(screen.queryByAltText('mail')).not.toBeInTheDocument();
        expect(screen.queryByAltText('svg')).not.toBeInTheDocument();
        expect(fetchMock).not.toHaveBeenCalled();
    });
});
