import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useSessionStore } from '@/store/sessionStore';
import { FileAutoComplete } from './FileAutoComplete';

function deferred<T>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>(done => { resolve = done; });
    return { promise, resolve };
}

const response = (path: string) => ({
    ok: true,
    json: async () => [{
        path,
        name: 'README.md',
        type: 'file',
        score: 1,
    }],
});

describe('FileAutoComplete', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        useSessionStore.setState({ sessionId: 'session-a' });
    });

    afterEach(() => {
        cleanup();
        vi.useRealTimers();
        vi.unstubAllGlobals();
        useSessionStore.setState({ sessionId: null });
    });

    it('does not search until a Session defines the file boundary', async () => {
        const fetchMock = vi.fn();
        vi.stubGlobal('fetch', fetchMock);
        useSessionStore.setState({ sessionId: null });

        render(
            <FileAutoComplete
                query="read"
                onSelect={vi.fn()}
                onClose={vi.fn()}
            />,
        );
        await act(async () => {
            await vi.advanceTimersByTimeAsync(200);
        });

        expect(fetchMock).not.toHaveBeenCalled();
    });

    it('searches files with the current Session id', async () => {
        const fetchMock = vi.fn().mockResolvedValue(
            response('README.md'));
        vi.stubGlobal('fetch', fetchMock);

        render(
            <FileAutoComplete
                query="read me"
                onSelect={vi.fn()}
                onClose={vi.fn()}
            />,
        );
        await act(async () => {
            await vi.advanceTimersByTimeAsync(151);
        });

        expect(fetchMock).toHaveBeenCalledWith(
            '/api/files/search?query=read+me&limit=15&sessionId=session-a',
            { signal: expect.any(AbortSignal) },
        );
        expect(screen.getByText('README.md')).toBeInTheDocument();
    });

    it('ignores a stale response after the active Session changes', async () => {
        const requestA = deferred<ReturnType<typeof response>>();
        const requestB = deferred<ReturnType<typeof response>>();
        const fetchMock = vi.fn()
            .mockReturnValueOnce(requestA.promise)
            .mockReturnValueOnce(requestB.promise);
        vi.stubGlobal('fetch', fetchMock);

        render(
            <FileAutoComplete
                query="read"
                onSelect={vi.fn()}
                onClose={vi.fn()}
            />,
        );
        await act(async () => {
            await vi.advanceTimersByTimeAsync(151);
        });

        act(() => {
            useSessionStore.setState({ sessionId: 'session-b' });
        });
        await act(async () => {
            await vi.advanceTimersByTimeAsync(151);
        });

        await act(async () => {
            requestB.resolve(response('project-b/README.md'));
            await requestB.promise;
        });
        expect(screen.getByText('project-b/README.md'))
            .toBeInTheDocument();

        await act(async () => {
            requestA.resolve(response('project-a/README.md'));
            await requestA.promise;
        });
        expect(screen.queryByText('project-a/README.md'))
            .not.toBeInTheDocument();
        expect(screen.getByText('project-b/README.md'))
            .toBeInTheDocument();
    });
});
