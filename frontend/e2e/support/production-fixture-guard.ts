import { fileURLToPath } from 'node:url';
import path from 'node:path';

export const PRODUCTION_FIXTURE_KIND = 'zkcode-production-e2e';
const configPath = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../playwright.production.config.ts');

/** Fail before a test can write through a development server's proxy. */
export function assertProductionFixture(configFile: string | undefined, baseURL: string | undefined,
    metadata: Record<string, unknown>): void {
    const fixture = metadata.zkProductionFixture as { kind?: string; portBase?: number } | undefined;
    const portBase = fixture?.portBase;
    if (!configFile || path.resolve(configFile) !== configPath
            || fixture?.kind !== PRODUCTION_FIXTURE_KIND
            || typeof portBase !== 'number' || !Number.isInteger(portBase)
            || portBase < 1024 || portBase + 2 > 65535
            || baseURL !== `http://127.0.0.1:${portBase + 2}`) {
        throw new Error('Production E2E requires playwright.production.config.ts and its isolated fixture URL');
    }
}
