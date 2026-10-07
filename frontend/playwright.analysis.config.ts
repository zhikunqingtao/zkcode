import { defineConfig, devices } from '@playwright/test';

const base = Number(process.env.ZK_ANALYSIS_E2E_PORT_BASE ?? (22000 + process.pid % 10000 * 2));
if (base < 1024 || base + 1 > 65535) throw new Error('Invalid analysis E2E ports');
process.env.ZK_ANALYSIS_E2E_PORT_BASE = String(base);
export default defineConfig({
    testDir: './e2e', testMatch: 'analysis-backend.spec.ts',
    outputDir: '../docs/test-results/screenshots/analysis-backend',
    fullyParallel: false, forbidOnly: true, retries: 0, workers: 1, timeout: 90_000,
    expect: { timeout: 30_000 }, reporter: [['list']],
    use: { ...devices['Desktop Chrome'], channel: 'chrome', baseURL: `http://127.0.0.1:${base + 1}`, trace: 'retain-on-failure', screenshot: 'only-on-failure' },
    webServer: [
        { command: `ZK_E2E_SERVER_PORT=${base} ZK_E2E_FRONTEND_PORT=${base + 1} node e2e/support/start-analysis-backend.mjs`, url: `http://127.0.0.1:${base}/api/analysis/openapi/python`, timeout: 120_000, reuseExistingServer: false, stdout: 'pipe', gracefulShutdown: { signal: 'SIGTERM', timeout: 30_000 } },
        { command: `VITE_API_URL=http://127.0.0.1:${base} npm run dev -- --host 127.0.0.1 --port ${base + 1} --strictPort`, url: `http://127.0.0.1:${base + 1}`, timeout: 30_000, reuseExistingServer: false },
    ],
});
