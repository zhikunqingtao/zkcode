interface PythonServiceErrorBody {
    code?: string;
    message?: string;
}

/** Convert sidecar proxy failures into a stable, user-facing unavailable state. */
export async function ensurePythonPanelResponse(response: Response): Promise<void> {
    if (response.ok) return;

    let body: PythonServiceErrorBody = {};
    try {
        body = await response.clone().json() as PythonServiceErrorBody;
    } catch {
        // Non-JSON upstream errors retain the generic HTTP fallback below.
    }
    if (response.status === 503) {
        throw new Error(`Python service unavailable${body.code ? ` (${body.code})` : ''}`);
    }
    if (response.status === 504) {
        throw new Error(`Python service timed out${body.code ? ` (${body.code})` : ''}`);
    }
    throw new Error(body.message ?? `HTTP ${response.status}: ${response.statusText}`);
}
