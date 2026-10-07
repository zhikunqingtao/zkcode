import type { ExternalResourceResult } from '@/types';

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function structuredResultSchema(
    metadata: Record<string, unknown> | undefined,
): string | null {
    if (!isRecord(metadata) || !isRecord(metadata.structuredResult)) return null;
    return typeof metadata.structuredResult.schema === 'string'
        ? metadata.structuredResult.schema
        : null;
}

/** Parse the allowlisted external-resource/v1 contract without trusting arbitrary tool metadata. */
export function parseExternalResourceResult(
    metadata: Record<string, unknown> | undefined,
): ExternalResourceResult | null {
    if (!isRecord(metadata)) return null;
    const raw = metadata.structuredResult;
    if (!isRecord(raw)
        || raw.schema !== 'external-resource/v1'
        || raw.kind !== 'download'
        || typeof raw.provider !== 'string'
        || typeof raw.url !== 'string'
        || typeof raw.label !== 'string'
        || typeof raw.size !== 'number'
        || typeof raw.sha256 !== 'string'
        || typeof raw.objectKey !== 'string'
        || typeof raw.mimeType !== 'string'
        || typeof raw.permanentlyPublic !== 'boolean'
        || typeof raw.downloadExpected !== 'boolean') {
        return null;
    }
    if (!raw.provider.trim() || !raw.label.trim() || !raw.objectKey.trim()
        || !raw.mimeType.trim() || !Number.isSafeInteger(raw.size) || raw.size < 0
        || !/^[a-f0-9]{64}$/i.test(raw.sha256)) {
        return null;
    }

    let parsed: URL;
    try {
        parsed = new URL(raw.url);
    } catch {
        return null;
    }
    if (parsed.protocol !== 'https:' || parsed.username || parsed.password || parsed.hash) {
        return null;
    }
    try {
        if (decodeURIComponent(parsed.pathname.replace(/^\//, '')) !== raw.objectKey) return null;
    } catch {
        return null;
    }

    return {
        schema: 'external-resource/v1',
        kind: 'download',
        provider: raw.provider,
        artifactId: typeof raw.artifactId === 'string' ? raw.artifactId : undefined,
        url: raw.url,
        label: raw.label,
        size: raw.size,
        sha256: raw.sha256,
        objectKey: raw.objectKey,
        mimeType: raw.mimeType,
        permanentlyPublic: raw.permanentlyPublic,
        downloadExpected: raw.downloadExpected,
    };
}

/** UI-only snapshot from a completed Edit, not a reconstruction from tool input. */
export function parseEditDiffResult(metadata: Record<string, unknown> | undefined): {
    filePath: string; diff: string; truncated: boolean;
} | null {
    if (!isRecord(metadata) || !isRecord(metadata.structuredResult)) return null;
    const raw = metadata.structuredResult;
    if (raw.schema !== 'edit-diff/v1' || typeof raw.filePath !== 'string' || !raw.filePath.trim()
        || typeof raw.diff !== 'string' || raw.diff.length > 64 * 1024
        || raw.diff.split('\n').length > 500 || typeof raw.truncated !== 'boolean') return null;
    return { filePath: raw.filePath, diff: raw.diff, truncated: raw.truncated };
}
