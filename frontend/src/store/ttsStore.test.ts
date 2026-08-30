import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useTtsStore } from './ttsStore';

function deferred<T>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>(done => { resolve = done; });
    return { promise, resolve };
}

function audioResponse(url: string): Response {
    return {
        ok: true,
        status: 200,
        json: async () => ({ audioUrl: url }),
    } as Response;
}

describe('ttsStore', () => {
    beforeEach(() => {
        useTtsStore.getState().stop();
        vi.restoreAllMocks();
    });

    afterEach(() => vi.unstubAllGlobals());

    it('ignores a stale JSON body when a newer message takes over', async () => {
        const firstJson = deferred<{ audioUrl: string }>();
        let firstJsonStarted = false;
        vi.stubGlobal('fetch', vi.fn()
            .mockResolvedValueOnce({
                ok: true,
                status: 200,
                json: () => {
                    firstJsonStarted = true;
                    return firstJson.promise;
                },
            } as Response)
            .mockResolvedValueOnce(audioResponse('https://example.invalid/second.mp3')));

        const created: string[] = [];
        class MockAudio {
            onended: (() => void) | null = null;
            onerror: (() => void) | null = null;
            constructor(url: string) { created.push(url); }
            play = vi.fn().mockResolvedValue(undefined);
            pause = vi.fn();
        }
        vi.stubGlobal('Audio', MockAudio);

        const playingFirst = useTtsStore.getState().play('first', '第一条');
        await vi.waitFor(() => expect(firstJsonStarted).toBe(true));
        const playingSecond = useTtsStore.getState().play('second', '第二条');
        firstJson.resolve({ audioUrl: 'https://example.invalid/first.mp3' });
        await Promise.all([playingFirst, playingSecond]);

        expect(created).toEqual(['https://example.invalid/second.mp3']);
        expect(useTtsStore.getState()).toMatchObject({
            playingMessageId: 'second',
            playState: 'playing',
        });
    });
});
