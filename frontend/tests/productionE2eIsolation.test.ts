// @vitest-environment node
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { assertProductionFixture, PRODUCTION_FIXTURE_KIND } from '../e2e/support/production-fixture-guard';

const metadata = { zkProductionFixture: { kind: PRODUCTION_FIXTURE_KIND, portBase: 24100 } };
const productionConfig = path.resolve('playwright.production.config.ts');

describe('production E2E entry points', () => {
    it('does not collect fixture-only tests through the default entry point', () => {
        // Collection only: Playwright never starts webServer or executes test bodies.
        const listed = execFileSync(process.execPath, [
            path.resolve('node_modules/@playwright/test/cli.js'), 'test',
            'mobile-prompt-bar.spec.ts|production-backend.spec.ts', '--list',
            '--pass-with-no-tests', '--reporter=list',
        ], { cwd: process.cwd(), encoding: 'utf8' });
        expect(listed).not.toContain('real backend');
        expect(listed).toContain('Total: 0 tests');
    });

    it('still collects production tests through the dedicated config', () => {
        const listed = execFileSync(process.execPath, [
            path.resolve('node_modules/@playwright/test/cli.js'), 'test',
            '--config', productionConfig, '--list', '--reporter=list',
        ], { cwd: process.cwd(), encoding: 'utf8' });
        expect(listed).toContain('mobile-prompt-bar.spec.ts');
        expect(listed).toContain('production-backend.spec.ts');
    });

    it('accepts only the dedicated config and its generated local URL', () => {
        expect(() => assertProductionFixture(productionConfig, 'http://127.0.0.1:24102', metadata)).not.toThrow();
        for (const [config, url, marker] of [
            [path.resolve('playwright.config.ts'), 'http://127.0.0.1:24102', metadata],
            [productionConfig, 'http://localhost:5273', metadata],
            [productionConfig, 'http://127.0.0.1:24102', {}],
            [productionConfig, 'http://127.0.0.1:24102', { zkProductionFixture: { kind: PRODUCTION_FIXTURE_KIND, portBase: 0 } }],
        ] as const) {
            expect(() => assertProductionFixture(config, url, marker)).toThrow('isolated fixture URL');
        }
    });

    it('runs the guard before a test body can issue its first request', () => {
        const directory = mkdtempSync(path.join(tmpdir(), 'zkcode-e2e-guard-'));
        try {
            const marker = path.join(directory, 'body-ran');
            const helper = path.resolve('e2e/support/production-test.ts');
            writeFileSync(path.join(directory, 'package.json'), '{"type":"module"}');
            writeFileSync(path.join(directory, 'guard.spec.ts'), `
                import { test } from ${JSON.stringify(helper)};
                import { writeFileSync } from 'node:fs';
                test('must not reach its first request', async ({ request }) => {
                    writeFileSync(${JSON.stringify(marker)}, 'body executed');
                    throw new Error('test body executed');
                });
            `);
            const config = path.join(directory, 'wrong.config.cjs');
            writeFileSync(config, `module.exports = {
                testDir: ${JSON.stringify(directory)}, workers: 1,
                use: { baseURL: 'http://localhost:5273' },
                outputDir: ${JSON.stringify(path.join(directory, 'output'))},
                reporter: [['list']]
            };`);
            let output = '';
            try {
                execFileSync(process.execPath, [path.resolve('node_modules/@playwright/test/cli.js'),
                    'test', '--config', config], { cwd: process.cwd(), encoding: 'utf8', stdio: 'pipe' });
            } catch (error) {
                output = String((error as { stdout?: string }).stdout ?? '');
            }
            expect(output).toContain('Production E2E requires playwright.production.config.ts');
            expect(() => readFileSync(marker)).toThrow();
        } finally {
            rmSync(directory, { recursive: true, force: true });
        }
    });
});
