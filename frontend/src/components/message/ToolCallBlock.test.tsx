import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import ToolCallBlock from './ToolCallBlock';

describe('ToolCallBlock structured result renderer', () => {
    it('uses the exact URL returned by the tool for the download link', () => {
        const objectKey = 'zhikuncode-artifacts/session/artifact/report.html';
        const url = `https://zhikunshare.oss-cn-beijing.aliyuncs.com/${objectKey}?version=1`;

        render(<ToolCallBlock
            toolUseId="publish-1"
            toolCall={{
                toolName: 'PublishArtifact',
                input: { file_path: 'report.html' },
                status: 'completed',
                startTime: 1,
                duration: 10,
                result: {
                    content: '{"status":"published"}',
                    isError: false,
                    metadata: {
                        structuredResult: {
                            schema: 'external-resource/v1',
                            kind: 'download',
                            provider: 'oss',
                            artifactId: 'artifact-1',
                            url,
                            label: 'report.html',
                            size: 2048,
                            sha256: 'c'.repeat(64),
                            objectKey,
                            mimeType: 'text/html',
                            permanentlyPublic: true,
                            downloadExpected: true,
                        },
                    },
                },
            }}
        />);

        expect(screen.getByTestId('external-resource-card')).toBeInTheDocument();
        expect(screen.getByTestId('external-resource-download').getAttribute('href')).toBe(url);
        expect(screen.queryByText(url)).not.toBeInTheDocument();
    });
});
