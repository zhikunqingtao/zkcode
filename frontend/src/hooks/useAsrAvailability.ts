import { useEffect } from 'react';
import {
    refreshSpeechAvailability,
    useSpeechAvailabilityStore,
} from '@/store/speechAvailabilityStore';

export function useAsrAvailability(): boolean {
    const available = useSpeechAvailabilityStore(state => state.asrAvailable);

    useEffect(() => {
        void refreshSpeechAvailability();
    }, []);

    return available;
}
