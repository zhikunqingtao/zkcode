import { create } from 'zustand';

export type TtsPlayState = 'idle' | 'loading' | 'playing' | 'error';

interface TtsStoreState {
    playingMessageId: string | null;
    playState: TtsPlayState;
    error: string | null;
    play: (messageId: string, text: string) => Promise<void>;
    stop: () => void;
}

let currentAudio: HTMLAudioElement | null = null;
let currentAbort: AbortController | null = null;
let generation = 0;

function disposeCurrent(): void {
    currentAbort?.abort();
    currentAbort = null;
    if (currentAudio) {
        currentAudio.onended = null;
        currentAudio.onerror = null;
        currentAudio.pause();
        currentAudio = null;
    }
}

function ttsError(status: number): string {
    if (status === 503) return '请先配置 DashScope API Key';
    if (status === 504) return '朗读生成超时';
    if (status === 429) return '服务繁忙，请稍后重试';
    return '朗读失败，请重试';
}

export const useTtsStore = create<TtsStoreState>((set, get) => ({
    playingMessageId: null,
    playState: 'idle',
    error: null,

    stop: () => {
        generation += 1;
        disposeCurrent();
        set({ playingMessageId: null, playState: 'idle', error: null });
    },

    play: async (messageId, text) => {
        get().stop();
        const ownGeneration = ++generation;
        const controller = new AbortController();
        currentAbort = controller;
        set({ playingMessageId: messageId, playState: 'loading', error: null });

        const timeout = setTimeout(() => controller.abort(), 65_000);
        try {
            const response = await fetch('/api/tts/synthesize', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ text }),
                signal: controller.signal,
            });
            if (ownGeneration !== generation) return;
            if (!response.ok) throw new Error(ttsError(response.status));
            const payload = await response.json() as { audioUrl?: unknown };
            if (ownGeneration !== generation) return;
            if (typeof payload.audioUrl !== 'string' || !payload.audioUrl) {
                throw new Error('朗读服务返回无效');
            }

            const audio = new Audio(payload.audioUrl);
            currentAudio = audio;
            audio.onended = () => {
                if (ownGeneration !== generation || currentAudio !== audio) return;
                currentAudio = null;
                set({ playingMessageId: null, playState: 'idle', error: null });
            };
            audio.onerror = () => {
                if (ownGeneration !== generation || currentAudio !== audio) return;
                currentAudio = null;
                set({ playingMessageId: messageId, playState: 'error', error: '音频播放失败' });
            };

            await audio.play();
            if (ownGeneration !== generation || currentAudio !== audio) {
                audio.pause();
                return;
            }
            currentAbort = null;
            set({ playingMessageId: messageId, playState: 'playing', error: null });
        } catch (caught) {
            if (ownGeneration !== generation) return;
            currentAbort = null;
            if (currentAudio) {
                currentAudio.pause();
                currentAudio = null;
            }
            const aborted = caught instanceof Error && caught.name === 'AbortError';
            set({
                playingMessageId: messageId,
                playState: 'error',
                error: aborted ? '朗读生成超时' : (caught instanceof Error ? caught.message : '朗读失败，请重试'),
            });
        } finally {
            clearTimeout(timeout);
        }
    },
}));
