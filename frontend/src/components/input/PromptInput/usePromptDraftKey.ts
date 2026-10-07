import { resolvePromptDraftKey } from '@/store/promptDraftStore';

/** Selecting a composer never transfers draft ownership. New-session binds do. */
export function usePromptDraftKey(sessionId?: string | null): string {
    return resolvePromptDraftKey(sessionId);
}
