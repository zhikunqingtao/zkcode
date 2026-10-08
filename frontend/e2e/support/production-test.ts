import { test as base, expect } from '@playwright/test';
import { assertProductionFixture } from './production-fixture-guard';

export const test = base.extend<{ productionFixtureGuard: void }>({
    productionFixtureGuard: [async ({ baseURL }, use, testInfo) => {
        assertProductionFixture(testInfo.config.configFile, baseURL, testInfo.config.metadata);
        await use();
    }, { auto: true }],
});
export { expect };
