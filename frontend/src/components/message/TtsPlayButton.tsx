import React from 'react';
import { Loader2, Square, Volume2 } from 'lucide-react';
import { useTtsStore } from '@/store/ttsStore';

interface TtsPlayButtonProps {
    messageId: string;
    text: string;
}

const TtsPlayButton: React.FC<TtsPlayButtonProps> = ({ messageId, text }) => {
    const playingMessageId = useTtsStore(state => state.playingMessageId);
    const playState = useTtsStore(state => state.playState);
    const error = useTtsStore(state => state.error);
    const play = useTtsStore(state => state.play);
    const stop = useTtsStore(state => state.stop);
    const current = playingMessageId === messageId;
    const loading = current && playState === 'loading';
    const playing = current && playState === 'playing';
    const failed = current && playState === 'error';
    const title = loading || playing ? '停止朗读' : failed ? (error ?? '朗读失败') : '朗读';

    React.useEffect(() => () => {
        const store = useTtsStore.getState();
        if (store.playingMessageId === messageId) store.stop();
    }, [messageId]);

    return (
        <button
            type="button"
            onClick={() => loading || playing ? stop() : void play(messageId, text)}
            disabled={!text.trim()}
            aria-label={title}
            title={title}
            className={`shrink-0 rounded p-1 transition-colors disabled:opacity-40
                ${playing ? 'text-red-400 hover:bg-red-500/10'
                    : failed ? 'text-red-400 hover:bg-red-500/10'
                        : 'text-[var(--text-muted)] hover:bg-[var(--bg-hover)] hover:text-[var(--text-primary)]'}`}
        >
            {loading ? <Loader2 size={14} className="animate-spin" />
                : playing ? <Square size={12} fill="currentColor" />
                    : <Volume2 size={14} />}
        </button>
    );
};

export default React.memo(TtsPlayButton);
