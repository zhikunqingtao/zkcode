import { create } from 'zustand';

interface SpeechAvailabilityState {
    asrAvailable: boolean;
    ttsAvailable: boolean;
    checked: boolean;
    checking: boolean;
}

const INITIAL_STATE: SpeechAvailabilityState = {
    asrAvailable: false,
    ttsAvailable: false,
    checked: false,
    checking: false,
};

export const useSpeechAvailabilityStore = create<SpeechAvailabilityState>(() => INITIAL_STATE);

let generation = 0;
let inFlight: { generation: number; controller: AbortController; promise: Promise<void> } | null = null;
let retryTimer: ReturnType<typeof setTimeout> | null = null;
const RETRY_DELAY_MS = 15_000;

interface AvailabilityResult {
    available: boolean;
    reachable: boolean;
}

function browserSupportsAsr(): boolean {
    return typeof window !== 'undefined'
        && window.isSecureContext === true
        && typeof navigator !== 'undefined'
        && typeof navigator.mediaDevices?.getUserMedia === 'function'
        && typeof MediaRecorder !== 'undefined';
}

function isAbortError(error: unknown): boolean {
    return error instanceof Error && error.name === 'AbortError';
}

async function fetchAvailability(path: string, signal: AbortSignal): Promise<AvailabilityResult> {
    try {
        const response = await fetch(path, { signal });
        if (!response.ok) return { available: false, reachable: false };
        const payload = await response.json() as { available?: unknown };
        return { available: payload.available === true, reachable: true };
    } catch (error) {
        if (isAbortError(error)) throw error;
        return { available: false, reachable: false };
    }
}

function clearRetryTimer(): void {
    if (retryTimer) clearTimeout(retryTimer);
    retryTimer = null;
}

function scheduleRetry(): void {
    if (retryTimer) return;
    retryTimer = setTimeout(() => {
        retryTimer = null;
        void refreshSpeechAvailability();
    }, RETRY_DELAY_MS);
}

/**
 * Refresh both speech capability flags. Concurrent component mounts share one
 * request; a forced refresh aborts and supersedes any older result.
 */
export function refreshSpeechAvailability(force = false): Promise<void> {
    const state = useSpeechAvailabilityStore.getState();
    if (!force && state.checked) return Promise.resolve();
    if (!force && inFlight) return inFlight.promise;

    if (force) {
        clearRetryTimer();
        if (inFlight) inFlight.controller.abort();
    }

    const requestGeneration = ++generation;
    const controller = new AbortController();
    useSpeechAvailabilityStore.setState(force
        ? { checking: true, checked: false, asrAvailable: false, ttsAvailable: false }
        : { checking: true, checked: false });

    const promise = Promise.all([
        browserSupportsAsr()
            ? fetchAvailability('/api/asr/status', controller.signal)
            : Promise.resolve({ available: false, reachable: true }),
        fetchAvailability('/api/tts/status', controller.signal),
    ]).then(([asr, tts]) => {
        if (requestGeneration !== generation) return;
        const transientFailure = !asr.reachable || !tts.reachable;
        useSpeechAvailabilityStore.setState({
            asrAvailable: asr.available,
            ttsAvailable: tts.available,
            checked: !transientFailure,
            checking: false,
        });
        if (transientFailure) scheduleRetry();
        else clearRetryTimer();
    }).catch((error: unknown) => {
        if (requestGeneration !== generation) return;
        if (!isAbortError(error)) {
            useSpeechAvailabilityStore.setState({
                asrAvailable: false,
                ttsAvailable: false,
                checked: false,
                checking: false,
            });
            scheduleRetry();
        }
    }).finally(() => {
        if (inFlight?.generation === requestGeneration) inFlight = null;
    });

    inFlight = { generation: requestGeneration, controller, promise };
    return promise;
}

/** Re-check immediately after the DashScope credential is added or removed. */
export function invalidateSpeechAvailability(): Promise<void> {
    return refreshSpeechAvailability(true);
}
