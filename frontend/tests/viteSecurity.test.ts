// @vitest-environment node

import { describe, expect, it } from 'vitest';
import { readFile } from 'node:fs/promises';
import viteConfig from '../vite.config';

describe('Local development service security', () => {
    it('binds the localhost-trusting proxy to loopback only', async () => {
        const config = await viteConfig({
            command: 'serve',
            mode: 'development',
            isSsrBuild: false,
            isPreview: false,
        });

        expect(config.server?.host).toBe('127.0.0.1');
        const proxy = config.server?.proxy;
        const backendTarget = proxy?.['/api']?.target;
        expect(backendTarget).toMatch(/^http:\/\/(localhost|127\.0\.0\.1):\d+$/);
        for (const route of [
            '/api/files/search',
            '/api/git',
            '/api/files',
            '/api/code-quality',
            '/api/analysis',
        ]) {
            expect(proxy?.[route]).toMatchObject({
                target: backendTarget,
            });
            expect(proxy?.[route]?.target).not.toMatch(/:8000$/);
        }
    });

    it('starts the internal Python service on UDS without a TCP host', async () => {
        const sidecarSource = await readFile(
            new URL('../../crates/zk-server/src/python/sidecar.rs', import.meta.url),
            'utf8',
        );

        expect(sidecarSource).toMatch(/\.arg\("--uds"\)/);
        expect(sidecarSource).not.toMatch(/\.arg\("--host"\)/);
    });
});
