import React from 'react';
import { Loader2, Mic, Square, X } from 'lucide-react';
import { useVoiceRecorder } from '@/hooks/useVoiceRecorder';

interface VoiceInputButtonProps {
    onTranscript: (text: string) => void;
    disabled?: boolean;
}

function formatTime(seconds: number): string {
    return `${String(Math.floor(seconds / 60)).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}`;
}

const VoiceInputButton: React.FC<VoiceInputButtonProps> = ({ onTranscript, disabled = false }) => {
    const { state, elapsedSeconds, error, startRecording, stopRecording, cancelRecording } = useVoiceRecorder(onTranscript);
    const recording = state === 'recording';
    const busy = state === 'requesting' || state === 'transcribing';
    const title = recording
        ? '停止录音'
        : state === 'requesting'
            ? '取消麦克风权限请求'
            : state === 'transcribing'
                ? '取消语音识别'
                : error ?? '语音输入';

    return (
        <div className="relative flex items-center gap-1">
            <button
                type="button"
                onClick={() => recording ? stopRecording() : busy ? cancelRecording() : startRecording()}
                disabled={disabled}
                aria-label={title}
                title={title}
                className={`shrink-0 p-2.5 rounded-lg transition-colors disabled:opacity-50
                    ${recording ? 'text-red-500 bg-red-500/10' : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800'}`}
            >
                {busy ? <span className="relative block"><Loader2 size={16} className="animate-spin" /><X size={10} className="absolute inset-0 m-auto" /></span>
                    : recording ? <Square size={13} fill="currentColor" />
                        : <Mic size={17} />}
            </button>
            {recording && (
                <>
                    <span className="flex h-4 items-center gap-0.5" aria-hidden="true">
                        {[6, 10, 8, 12].map((height, index) => (
                            <span
                                key={height}
                                className="animate-soundwave w-0.5 rounded-full bg-red-500"
                                style={{ height, animationDelay: `${index * 150}ms` }}
                            />
                        ))}
                    </span>
                    <span className="text-xs font-mono tabular-nums text-gray-400">{formatTime(elapsedSeconds)}</span>
                </>
            )}
            {state === 'error' && error && (
                <span role="alert" className="absolute bottom-full right-0 mb-2 whitespace-nowrap rounded bg-red-950 px-2 py-1 text-xs text-red-300">
                    {error}
                </span>
            )}
        </div>
    );
};

export default React.memo(VoiceInputButton);
