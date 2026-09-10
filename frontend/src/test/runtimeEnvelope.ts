import { WS_PROTOCOL_VERSION } from '@/types';
import type { RuntimeEventContext, RuntimeServerEnvelope } from '@/types';

let nextFixtureEvent = 1;

/** Build a complete v4 envelope for dispatch-level tests. */
export function runtimeEnvelope(
    context: Partial<RuntimeEventContext> = {},
): RuntimeServerEnvelope {
    const ordinal = nextFixtureEvent++;
    return {
        ts: ordinal,
        eventContext: {
            protocolVersion: WS_PROTOCOL_VERSION,
            eventId: `fixture-event-${ordinal}`,
            sessionId: null,
            taskId: null,
            runId: null,
            sourceTaskId: null,
            sourceRunId: null,
            toolUseId: null,
            ...context,
        },
    };
}
