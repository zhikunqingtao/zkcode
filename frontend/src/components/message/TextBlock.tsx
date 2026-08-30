/**
 * TextBlock — Markdown 文本渲染组件
 *
 * SPEC: §8.2.4D 消息渲染管线
 * 使用 react-markdown 渲染 Markdown 内容，代码块使用 CodeBlock 语法高亮。
 * 支持流式更新 (streaming) 时附加闪烁光标。
 */

import React, { useEffect, useMemo, useState } from 'react';
import ReactMarkdown, { defaultUrlTransform } from 'react-markdown';
import type { Components, UrlTransform } from 'react-markdown';
import type { ElementContent } from 'hast';
import remarkGfm from 'remark-gfm';
import CodeBlock from './CodeBlock';
import ImageBlock from './ImageBlock';
import MermaidBlock from '../visualization/shared/MermaidBlock';
import { useSessionStore } from '@/store/sessionStore';

interface TextBlockProps {
    text: string;
    streaming?: boolean;
    /** Path of the Markdown file being rendered; relative images resolve from its directory. */
    sourcePath?: string;
}

const BASE64_RASTER_IMAGE = /^data:image\/(?:png|jpe?g|gif|webp|avif);base64,[a-z0-9+/]+={0,2}$/i;

function isBrowserImageSource(src: string): boolean {
    return /^(https?:|blob:)/i.test(src)
        || src.startsWith('//')
        || BASE64_RASTER_IMAGE.test(src);
}

function toWorkspacePath(src: string): string {
    let path = src;
    if (/^file:\/\//i.test(path)) {
        try {
            return decodeURIComponent(new URL(path).pathname);
        } catch {
            path = path.replace(/^file:\/\//i, '');
        }
    }
    try {
        return decodeURIComponent(path);
    } catch {
        return path;
    }
}

function isAbsoluteWorkspacePath(path: string): boolean {
    const normalized = path.replace(/\\/g, '/');
    return normalized.startsWith('/') || /^[a-z]:\//i.test(normalized);
}

function resolveWorkspacePath(src: string, sourcePath?: string): string {
    const workspacePath = toWorkspacePath(src);
    if (!sourcePath || isAbsoluteWorkspacePath(workspacePath)) return workspacePath;

    const normalizedSource = toWorkspacePath(sourcePath).replace(/\\/g, '/');
    const directoryBoundary = normalizedSource.lastIndexOf('/');
    if (directoryBoundary < 0) return workspacePath;
    return `${normalizedSource.slice(0, directoryBoundary + 1)}${workspacePath.replace(/\\/g, '/')}`;
}

const MarkdownImage: React.FC<{ src?: string; alt?: string; sourcePath?: string }> = ({
    src,
    alt,
    sourcePath,
}) => {
    const sessionId = useSessionStore(s => s.sessionId);
    const browserLoadable = !!src && isBrowserImageSource(src);
    const workspacePath = src && !browserLoadable ? resolveWorkspacePath(src, sourcePath) : null;
    const requestKey = workspacePath && sessionId ? `${sessionId}:${workspacePath}` : null;
    const [result, setResult] = useState<{
        requestKey: string;
        objectUrl?: string;
        error?: string;
    } | null>(null);

    useEffect(() => {
        if (!workspacePath || !sessionId || !requestKey) return;
        const controller = new AbortController();
        let url: string | null = null;
        setResult(null);
        void fetch(
            `/api/sessions/${encodeURIComponent(sessionId)}/files/preview?path=${encodeURIComponent(workspacePath)}`,
            { headers: { 'X-Session-Id': sessionId }, signal: controller.signal },
        )
            .then(async response => {
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const blob = await response.blob();
                if (controller.signal.aborted) return;
                url = URL.createObjectURL(blob);
                setResult({ requestKey, objectUrl: url });
            })
            .catch(fetchError => {
                if (!controller.signal.aborted) {
                    setResult({
                        requestKey,
                        error: fetchError instanceof Error ? fetchError.message : String(fetchError),
                    });
                }
            });
        return () => {
            controller.abort();
            if (url) URL.revokeObjectURL(url);
        };
    }, [requestKey, workspacePath, sessionId]);

    const currentResult = result?.requestKey === requestKey ? result : null;
    const error = currentResult?.error;
    const objectUrl = currentResult?.objectUrl;

    if (!src) return null;
    if (browserLoadable) return <ImageBlock src={src} alt={alt} />;
    if (error || !sessionId) {
        return (
            <span className="my-2 inline-flex items-center rounded-lg border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-sm text-amber-400">
                {error?.includes('403') ? '图片不在当前工作区范围内' : `图片加载失败 (${error ?? '无会话'})`}
                {workspacePath && <span className="ml-2 text-xs text-[var(--text-muted)]">{workspacePath}</span>}
            </span>
        );
    }
    if (!objectUrl) {
        return <span className="my-2 inline-block h-32 w-48 animate-pulse rounded-lg border border-[var(--border)] bg-[var(--bg-secondary)]" />;
    }
    return <ImageBlock src={objectUrl} alt={alt} />;
};

const urlTransform: UrlTransform = (url, key) => {
    if (key === 'src') {
        if (/^file:\/\//i.test(url) || /^[a-z]:[\\/]/i.test(url)) return url;
        if (isBrowserImageSource(url)) return url;
        if (/^[a-z][a-z0-9+.-]*:/i.test(url)) return '';
        return url;
    }
    return defaultUrlTransform(url);
};

function containsImageElement(children: ElementContent[]): boolean {
    return children.some(child => child.type === 'element'
        && (child.tagName === 'img' || containsImageElement(child.children)));
}

const TextBlock: React.FC<TextBlockProps> = ({ text, streaming = false, sourcePath }) => {
    const components: Components = useMemo(() => ({
        code({ className, children, ...props }) {
            const match = /language-(\w+)/.exec(className ?? '');
            const codeStr = String(children).replace(/\n$/, '');
            const lang = match?.[1];

            // Inline code
            if (!match && !codeStr.includes('\n')) {
                return (
                    <code
                        className="px-1.5 py-0.5 rounded bg-[var(--code-bg)] text-sm font-mono text-[var(--text-primary)]"
                        {...props}
                    >
                        {children}
                    </code>
                );
            }

            // Mermaid diagram
            if (lang === 'mermaid') {
                return <MermaidBlock code={codeStr} />;
            }

            // Fenced code block
            return <CodeBlock code={codeStr} language={lang} />;
        },
        // Headings
        h1: ({ children }) => <h1 className="text-2xl font-bold mt-6 mb-3">{children}</h1>,
        h2: ({ children }) => <h2 className="text-xl font-bold mt-5 mb-2">{children}</h2>,
        h3: ({ children }) => <h3 className="text-lg font-semibold mt-4 mb-2">{children}</h3>,
        // Paragraphs
        p: ({ node, children }) => node && containsImageElement(node.children)
            ? <div className="my-2 leading-relaxed">{children}</div>
            : <p className="my-2 leading-relaxed">{children}</p>,
        // Lists
        ul: ({ children }) => <ul className="list-disc pl-6 my-2 space-y-1">{children}</ul>,
        ol: ({ children }) => <ol className="list-decimal pl-6 my-2 space-y-1">{children}</ol>,
        li: ({ children }) => <li className="leading-relaxed">{children}</li>,
        // Blockquotes
        blockquote: ({ children }) => (
            <blockquote className="border-l-4 border-blue-500 pl-4 my-3 text-[var(--text-secondary)] italic">
                {children}
            </blockquote>
        ),
        // Links
        a: ({ href, children }) => (
            <a
                href={href}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-400 hover:text-blue-300 underline"
            >
                {children}
            </a>
        ),
        // Tables
        table: ({ children }) => (
            <div className="overflow-x-auto my-3">
                <table className="min-w-full border-collapse border border-[var(--border)] text-sm">
                    {children}
                </table>
            </div>
        ),
        th: ({ children }) => (
            <th className="border border-[var(--border)] px-3 py-2 bg-[var(--bg-secondary)] text-left font-semibold">
                {children}
            </th>
        ),
        td: ({ children }) => (
            <td className="border border-[var(--border)] px-3 py-2">{children}</td>
        ),
        // Horizontal rule
        hr: () => <hr className="my-4 border-[var(--border)]" />,
        // Strong / Em
        strong: ({ children }) => <strong className="font-bold">{children}</strong>,
        em: ({ children }) => <em className="italic">{children}</em>,
        img: ({ src, alt }) => <MarkdownImage src={src} alt={alt} sourcePath={sourcePath} />,
    }), [sourcePath]);

    return (
        <div className="text-block max-w-none text-sm text-[var(--text-primary)] leading-relaxed">
            <ReactMarkdown remarkPlugins={[remarkGfm]} components={components} urlTransform={urlTransform}>{text}</ReactMarkdown>
            {streaming && (
                <span className="inline-block w-2 h-4 ml-0.5 bg-blue-400 animate-pulse rounded-sm" />
            )}
        </div>
    );
};

export default React.memo(TextBlock);
