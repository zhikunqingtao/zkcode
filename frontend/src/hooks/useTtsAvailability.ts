import { useEffect } from 'react';
import {
    refreshSpeechAvailability,
    useSpeechAvailabilityStore,
} from '@/store/speechAvailabilityStore';

export function useTtsAvailability(): boolean {
    const available = useSpeechAvailabilityStore(state => state.ttsAvailable);

    useEffect(() => {
        void refreshSpeechAvailability();
    }, []);

    return available;
}
