import { fireEvent, render, screen } from '@testing-library/react';
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

        fireEvent.click(screen.getAllByRole('button')[0]);
        fireEvent.click(screen.getByRole('button', { name: 'Result' }));
        expect(screen.getByTestId('external-resource-card')).toBeInTheDocument();
        expect(screen.getByTestId('external-resource-download').getAttribute('href')).toBe(url);
        expect(screen.queryByText(url)).not.toBeInTheDocument();
    });

    it('keeps successful diagnostic output collapsed by default', () => {
        render(<ToolCallBlock
            toolUseId="search-1"
            toolCall={{
                toolName: 'WebSearch',
                input: { query: 'example' },
                status: 'completed',
                startTime: 1,
                duration: 10,
                result: {
                    content: 'large raw search payload',
                    isError: false,
                },
            }}
        />);

        fireEvent.click(screen.getAllByRole('button')[0]);
        expect(screen.getByRole('button', { name: 'Result' })).toBeInTheDocument();
        expect(screen.queryByText('large raw search payload')).not.toBeInTheDocument();
    });

    it('shows hook presentation as escaped notes while preserving the actual failure and result', () => {
        const content = 'Actual command failed: exit 7';
        const note = '<img src=x onerror=alert(1)> Everything succeeded';
        render(<ToolCallBlock toolUseId="hooked" expanded toolCall={{
            toolName: 'Bash', input: { command: 'false' }, status: 'error', startTime: 1,
            result: { content, isError: true, metadata: { hookPresentation: { text: note } } },
        }} />);
        expect(screen.getByRole('complementary', { name: 'Hook 展示备注' })).toHaveTextContent(note);
        expect(screen.queryByRole('img')).not.toBeInTheDocument();
        expect(screen.getByRole('button', { name: /Result.*error/ })).toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: /Result.*error/ }));
        expect(screen.getByText(content)).toBeInTheDocument();
        expect(screen.queryByText('Completed')).not.toBeInTheDocument();
    });

    it('ignores malformed presentation metadata without hiding the original output', () => {
        render(<ToolCallBlock toolUseId="malformed" expanded toolCall={{
            toolName: 'WebSearch', input: {}, status: 'completed', startTime: 1,
            result: { content: 'Original result', isError: false, metadata: { hookPresentation: { text: { content: 'wrong' } } } },
        }} />);
        expect(screen.queryByRole('complementary', { name: 'Hook 展示备注' })).not.toBeInTheDocument();
        fireEvent.click(screen.getByRole('button', { name: 'Result' }));
        expect(screen.getByText('Original result')).toBeInTheDocument();
    });
});
