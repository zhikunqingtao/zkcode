import { useCallback, useEffect, useRef, useState } from 'react';

export type VoiceState = 'idle' | 'requesting' | 'recording' | 'transcribing' | 'error';

const MIME_CANDIDATES = [
    'audio/webm;codecs=opus',
    'audio/webm',
    'audio/ogg;codecs=opus',
    'audio/mp4',
] as const;

function pickMimeType(): string {
    for (const mime of MIME_CANDIDATES) {
        if (MediaRecorder.isTypeSupported(mime)) return mime;
    }
    return '';
}

function extensionForMime(mime: string): string {
    if (mime.includes('ogg')) return 'ogg';
    if (mime.includes('mp4')) return 'mp4';
    return 'webm';
}

function recognitionError(status: number): string {
    if (status === 413) return '录音过大，请缩短后重试';
    if (status === 503) return '请先配置 DashScope API Key';
    if (status === 504) return '识别超时，请重试';
    return '语音识别失败，请重试';
}

export function useVoiceRecorder(
    onTranscript: (text: string) => void,
    maxDurationMs = 120_000,
) {
    const [state, setState] = useState<VoiceState>('idle');
    const [elapsedSeconds, setElapsedSeconds] = useState(0);
    const [error, setError] = useState<string | null>(null);
    const stateRef = useRef<VoiceState>('idle');
    const recorderRef = useRef<MediaRecorder | null>(null);
    const streamRef = useRef<MediaStream | null>(null);
    const chunksRef = useRef<Blob[]>([]);
    const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
    const maxTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const abortRef = useRef<AbortController | null>(null);
    const operationRef = useRef(0);
    const mountedRef = useRef(true);

    const setVoiceState = useCallback((next: VoiceState) => {
        stateRef.current = next;
        if (mountedRef.current) setState(next);
    }, []);

    const clearTimers = useCallback(() => {
        if (intervalRef.current) clearInterval(intervalRef.current);
        if (maxTimerRef.current) clearTimeout(maxTimerRef.current);
        intervalRef.current = null;
        maxTimerRef.current = null;
    }, []);

    const releaseStream = useCallback(() => {
        streamRef.current?.getTracks().forEach(track => track.stop());
        streamRef.current = null;
    }, []);

    const cancelRecording = useCallback(() => {
        operationRef.current += 1;
        clearTimers();
        const recorder = recorderRef.current;
        if (recorder) {
            recorder.onstop = null;
            recorder.ondataavailable = null;
            recorder.onerror = null;
            if (recorder.state !== 'inactive') recorder.stop();
        }
        recorderRef.current = null;
        releaseStream();
        abortRef.current?.abort();
        abortRef.current = null;
        chunksRef.current = [];
        setElapsedSeconds(0);
        setError(null);
        setVoiceState('idle');
    }, [clearTimers, releaseStream, setVoiceState]);

    const fail = useCallback((message: string) => {
        operationRef.current += 1;
        clearTimers();
        const recorder = recorderRef.current;
        if (recorder) {
            recorder.onstop = null;
            recorder.ondataavailable = null;
            recorder.onerror = null;
            if (recorder.state !== 'inactive') recorder.stop();
        }
        recorderRef.current = null;
        releaseStream();
        abortRef.current?.abort();
        abortRef.current = null;
        chunksRef.current = [];
        if (mountedRef.current) setError(message);
        setVoiceState('error');
    }, [clearTimers, releaseStream, setVoiceState]);

    const stopRecording = useCallback(() => {
        const recorder = recorderRef.current;
        if (!recorder || recorder.state === 'inactive' || stateRef.current !== 'recording') return;

        const operation = operationRef.current;
        clearTimers();
        setVoiceState('transcribing');
        recorder.onstop = () => {
            recorderRef.current = null;
            recorder.onerror = null;
            releaseStream();
            if (operation !== operationRef.current || !mountedRef.current) return;

            const mimeType = recorder.mimeType || 'audio/webm';
            const audio = new Blob(chunksRef.current, { type: mimeType });
            chunksRef.current = [];
            if (audio.size === 0) {
                fail('未录到音频，请重试');
                return;
            }

            const form = new FormData();
            form.append('audio', audio, `recording.${extensionForMime(mimeType)}`);
            const controller = new AbortController();
            abortRef.current = controller;
            let timedOut = false;
            const timeout = setTimeout(() => {
                timedOut = true;
                controller.abort();
            }, 65_000);

            void fetch('/api/asr/recognize', {
                method: 'POST',
                body: form,
                signal: controller.signal,
            }).then(async response => {
                if (!response.ok) throw new Error(recognitionError(response.status));
                const payload = await response.json() as { text?: unknown };
                if (typeof payload.text !== 'string') throw new Error('语音识别返回无效');
                return payload.text;
            }).then(text => {
                if (operation !== operationRef.current || !mountedRef.current) return;
                if (text) onTranscript(text);
                setVoiceState('idle');
                setElapsedSeconds(0);
            }).catch(caught => {
                if (operation !== operationRef.current || !mountedRef.current) return;
                if (caught instanceof Error && caught.name === 'AbortError' && !timedOut) return;
                fail(timedOut ? '识别超时，请重试' : (caught instanceof Error ? caught.message : '语音识别失败，请重试'));
            }).finally(() => {
                clearTimeout(timeout);
                if (abortRef.current === controller) abortRef.current = null;
            });
        };
        recorder.stop();
    }, [clearTimers, fail, onTranscript, releaseStream, setVoiceState]);

    const startRecording = useCallback(() => {
        if (stateRef.current !== 'idle' && stateRef.current !== 'error') return;
        if (!window.isSecureContext || !navigator.mediaDevices?.getUserMedia || typeof MediaRecorder === 'undefined') {
            fail('当前环境不支持安全的麦克风访问');
            return;
        }

        const operation = ++operationRef.current;
        setVoiceState('requesting');
        setElapsedSeconds(0);
        setError(null);
        chunksRef.current = [];

        void navigator.mediaDevices.getUserMedia({ audio: true }).then(stream => {
            if (operation !== operationRef.current || !mountedRef.current) {
                stream.getTracks().forEach(track => track.stop());
                return;
            }

            streamRef.current = stream;
            const mimeType = pickMimeType();
            let recorder: MediaRecorder;
            try {
                recorder = new MediaRecorder(stream, mimeType ? { mimeType } : undefined);
            } catch {
                fail('无法启动录音设备');
                return;
            }
            recorderRef.current = recorder;
            recorder.ondataavailable = event => {
                if (event.data.size > 0) chunksRef.current.push(event.data);
            };
            recorder.onerror = () => fail('录音失败，请重试');
            recorder.start(250);
            intervalRef.current = setInterval(() => setElapsedSeconds(value => value + 1), 1_000);
            maxTimerRef.current = setTimeout(() => stopRecording(), maxDurationMs);
            setVoiceState('recording');
        }).catch((caught: unknown) => {
            if (operation !== operationRef.current || !mountedRef.current) return;
            const name = caught instanceof DOMException ? caught.name : '';
            if (name === 'NotAllowedError') fail('需要麦克风权限，请在浏览器设置中允许');
            else if (name === 'NotFoundError') fail('未检测到麦克风设备');
            else fail('无法访问麦克风，请重试');
        });
    }, [fail, maxDurationMs, setVoiceState, stopRecording]);

    useEffect(() => {
        if (state !== 'error') return;
        const timer = setTimeout(() => {
            setError(null);
            setVoiceState('idle');
        }, 3_000);
        return () => clearTimeout(timer);
    }, [setVoiceState, state]);

    useEffect(() => {
        mountedRef.current = true;
        return () => {
            mountedRef.current = false;
            operationRef.current += 1;
            clearTimers();
            const recorder = recorderRef.current;
            if (recorder) {
                recorder.onstop = null;
                recorder.ondataavailable = null;
                recorder.onerror = null;
                if (recorder.state !== 'inactive') recorder.stop();
            }
            recorderRef.current = null;
            releaseStream();
            abortRef.current?.abort();
            abortRef.current = null;
        };
    }, [clearTimers, releaseStream]);

    return { state, elapsedSeconds, error, startRecording, stopRecording, cancelRecording };
}
