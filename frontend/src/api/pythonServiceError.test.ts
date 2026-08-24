import { describe, expect, it } from 'vitest';

import { ensurePythonPanelResponse } from './pythonServiceError';

describe('ensurePythonPanelResponse', () => {
    it('maps sidecar down to an explicit unavailable state', async () => {
        const response = new Response(JSON.stringify({
            code: 'PYTHON_SERVICE_UNAVAILABLE',
            message: 'Python service is unavailable',
        }), { status: 503, headers: { 'Content-Type': 'application/json' } });

        await expect(ensurePythonPanelResponse(response)).rejects.toThrow(
            'Python service unavailable (PYTHON_SERVICE_UNAVAILABLE)',
        );
    });

    it('maps timeout separately and accepts successful responses', async () => {
        await expect(ensurePythonPanelResponse(new Response('{}', { status: 200 })))
            .resolves.toBeUndefined();
        await expect(ensurePythonPanelResponse(new Response('{}', { status: 504 })))
            .rejects.toThrow('Python service timed out');
    });
});
