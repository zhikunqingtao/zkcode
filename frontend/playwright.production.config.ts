import { defineConfig, devices } from '@playwright/test';

const inheritedBase = process.env.ZK_E2E_PORT_BASE;
const explicitBase = Number.parseInt(inheritedBase ?? '', 10);
const generatedBase = 20_000 + (process.pid % 10_000) * 3;
const portBase = Number.isInteger(explicitBase) ? explicitBase : generatedBase;
if (portBase < 1024 || portBase + 2 > 65535) {
  throw new Error('ZK_E2E_PORT_BASE must leave room for three non-privileged ports');
}
// Playwright evaluates this config in both its coordinator and worker
// processes. Freeze the coordinator's generated base in the inherited
// environment so workers do not compute a different set of ports.
if (!inheritedBase) process.env.ZK_E2E_PORT_BASE = String(portBase);

const providerPort = portBase;
const serverPort = portBase + 1;
const frontendPort = portBase + 2;
const frontendUrl = `http://127.0.0.1:${frontendPort}`;

export default defineConfig({
  testDir: './e2e',
  testMatch: 'production-backend.spec.ts',
  outputDir: '../docs/test-results/screenshots/production-backend',
  fullyParallel: false,
  forbidOnly: true,
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  timeout: 90_000,
  expect: { timeout: 20_000 },
  reporter: [['list']],
  use: {
    ...devices['Desktop Chrome'],
    // zkcode is a macOS desktop product and the existing E2E contract uses the
    // installed Chrome channel. This avoids a second browser download in the
    // hermetic local gate.
    channel: 'chrome',
    baseURL: frontendUrl,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
  },
  webServer: [
    {
      command: `ZK_E2E_PROVIDER_PORT=${providerPort} node e2e/support/scripted-openai-provider.mjs`,
      url: `http://127.0.0.1:${providerPort}/health`,
      reuseExistingServer: false,
      timeout: 30_000,
    },
    {
      command: `ZK_E2E_SERVER_PORT=${serverPort} ZK_E2E_PROVIDER_PORT=${providerPort} ZK_E2E_FRONTEND_PORT=${frontendPort} ../scripts/testing/start-production-e2e-server.sh`,
      url: `http://127.0.0.1:${serverPort}/api/health/live`,
      reuseExistingServer: false,
      timeout: 240_000,
    },
    {
      command: `VITE_API_URL=http://127.0.0.1:${serverPort} npm run dev -- --host 127.0.0.1 --port ${frontendPort} --strictPort`,
      url: frontendUrl,
      reuseExistingServer: false,
      timeout: 30_000,
    },
  ],
});
