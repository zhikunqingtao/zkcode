import { describe, expect, it } from 'vitest';
import { parseExternalResourceResult } from './structuredToolResult';

function metadata(overrides: Record<string, unknown> = {}) {
    const objectKey = 'zhikuncode-artifacts/session/artifact/report.html';
    return {
        structuredResult: {
            schema: 'external-resource/v1',
            kind: 'download',
            provider: 'oss',
            artifactId: 'artifact-1',
            url: `https://zhikunshare.oss-cn-beijing.aliyuncs.com/${objectKey}`,
            label: 'report.html',
            size: 2048,
            sha256: 'b'.repeat(64),
            objectKey,
            mimeType: 'text/html',
            permanentlyPublic: true,
            downloadExpected: true,
            ...overrides,
        },
    };
}

describe('parseExternalResourceResult', () => {
    it('preserves the authoritative URL byte-for-byte', () => {
        const url = 'https://zhikunshare.oss-cn-beijing.aliyuncs.com/a%20b/report.html?version=1';
        const result = parseExternalResourceResult(metadata({
            url,
            objectKey: 'a b/report.html',
        }));

        expect(result?.url).toBe(url);
    });

    it('rejects non-HTTPS and object-key mismatches', () => {
        expect(parseExternalResourceResult(metadata({
            url: 'http://zhikunshare.oss-cn-beijing.aliyuncs.com/a',
            objectKey: 'a',
        }))).toBeNull();
        expect(parseExternalResourceResult(metadata({ objectKey: 'different/file.html' }))).toBeNull();
    });
});
