/**
 * CodeInsightStore — Git 数据管理
 * 管理 Git Log / Diff / Blame 数据状态
 */

import { create } from 'zustand';
import { AnalysisRequest, isAnalysisCancelled } from '@/api/analysisClient';
import { useSessionStore } from './sessionStore';
import { immer } from 'zustand/middleware/immer';
import { subscribeWithSelector } from 'zustand/middleware';

// ── 类型定义（基于 Python API 响应） ──

export interface GitCommitFile {
    path: string;
    additions: number;
    deletions: number;
    status: string;
}

export interface GitCommit {
    sha: string;
    message: string;
    author: string;
    date: string;
    files: string[];
}

export interface GitDiff {
    summary: string;
    detailed: string;
    files_changed: number;
}

export interface GitBlameLine {
    line_no: number;
    sha: string;
    author: string;
    date: string;
    content: string;
}

export interface GitBlame {
    file_path: string;
    lines: GitBlameLine[];
    total_lines: number;
}

interface GitApiResponse<T = unknown> {
    success: boolean;
    data: T | null;
    error_code: string | null;
    error_message: string | null;
}

// ── Store 状态 ──

export interface CodeInsightState {
    // Git Log
    gitCommits: GitCommit[];
    gitLoading: boolean;
    gitError: string | null;
    gitTotal: number;
    gitHead: string | null;

    // Git Diff
    activeDiff: GitDiff | null;
    diffLoading: boolean;
    diffError: string | null;

    // Git Blame
    activeBlame: GitBlame | null;
    blameLoading: boolean;
    blameError: string | null;

    // Actions
    fetchGitLog: (repoPath: string, maxCount?: number, branch?: string) => Promise<void>;
    fetchMoreGitLog: (repoPath: string, maxCount?: number, branch?: string) => Promise<void>;
    fetchGitDiff: (repoPath: string, ref1: string, ref2: string) => Promise<void>;
    fetchGitBlame: (repoPath: string, filePath: string, ref?: string) => Promise<void>;
    clearDiff: () => void;
    clearBlame: () => void;
    clearAll: () => void;
}

type Channel = 'log' | 'diff' | 'blame';
const pending = new Map<Channel, AnalysisRequest>();
function cancel(channel: Channel) { pending.get(channel)?.cancel(); pending.delete(channel); }
async function gitApiPost<T>(channel: Channel, repoPath: string, body: Record<string, unknown>, apply: (data: T) => void, fail: (message: string | null) => void): Promise<void> {
    cancel(channel);
    let request: AnalysisRequest | null = null;
    try {
        request = new AnalysisRequest(repoPath, '/api/git/cancel');
        pending.set(channel, request);
        const response = await request.post<GitApiResponse<T>>(`/api/git/${channel}`, body);
        if (pending.get(channel) !== request) return;
        if (!response.success || !response.data) throw new Error(response.error_message ?? 'Git 服务返回无效结果');
        apply(response.data);
    } catch (error) {
        if (request && pending.get(channel) !== request) return;
        fail(isAnalysisCancelled(error) ? null : error instanceof Error ? error.message : String(error));
    } finally { if (pending.get(channel) === request) pending.delete(channel); }
}

export const useCodeInsightStore = create<CodeInsightState>()(
    subscribeWithSelector(immer((set, get) => ({
        gitCommits: [],
        gitLoading: false,
        gitError: null,
        gitTotal: 0,
        gitHead: null,

        activeDiff: null,
        diffLoading: false,
        diffError: null,

        activeBlame: null,
        blameLoading: false,
        blameError: null,

        fetchGitLog: async (repoPath, maxCount = 20, branch) => {
            set(d => { d.gitLoading = true; d.gitError = null; d.gitCommits = []; d.gitTotal = 0; d.gitHead = null; });
            await gitApiPost<{ commits: GitCommit[]; total: number; head: string }>('log', repoPath, { maxCount, branch }, data => {
                if (!Array.isArray(data.commits) || !Number.isSafeInteger(data.total) || data.total < data.commits.length || typeof data.head !== 'string') throw new Error('Git 日志格式无效');
                set(d => { d.gitCommits = data.commits; d.gitTotal = data.total; d.gitHead = data.head; d.gitLoading = false; });
            }, error => set(d => { d.gitError = error; d.gitLoading = false; }));
        },
        fetchMoreGitLog: async (repoPath, maxCount = 20) => {
            if (get().gitLoading || !get().gitHead) return;
            const offset = get().gitCommits.length;
            set(d => { d.gitLoading = true; d.gitError = null; });
            await gitApiPost<{ commits: GitCommit[]; total: number; head: string }>('log', repoPath, { maxCount, offset, branch: get().gitHead }, data => {
                if (!Array.isArray(data.commits) || data.head !== get().gitHead || data.commits.some(item => get().gitCommits.some(existing => existing.sha === item.sha))) throw new Error('Git 分页结果与当前快照不一致');
                set(d => { d.gitCommits.push(...data.commits); d.gitTotal = data.total; d.gitLoading = false; });
            }, error => set(d => { d.gitError = error; d.gitLoading = false; }));
        },
        fetchGitDiff: async (repoPath, _ref1, ref2) => {
            set(d => { d.activeDiff = null; d.diffLoading = true; d.diffError = null; });
            await gitApiPost<GitDiff>('diff', repoPath, { commit: ref2 }, data => {
                if (typeof data.detailed !== 'string' || typeof data.summary !== 'string' || !Number.isSafeInteger(data.files_changed)) throw new Error('Git 差异格式无效');
                set(d => { d.activeDiff = data; d.diffLoading = false; });
            }, error => set(d => { d.diffError = error; d.diffLoading = false; }));
        },
        fetchGitBlame: async (repoPath, filePath, ref) => {
            set(d => { d.activeBlame = null; d.blameLoading = true; d.blameError = null; });
            await gitApiPost<GitBlame>('blame', repoPath, { filePath, ref }, data => {
                if (data.file_path !== filePath || !Array.isArray(data.lines) || data.total_lines !== data.lines.length) throw new Error('Git Blame 格式无效');
                set(d => { d.activeBlame = data; d.blameLoading = false; });
            }, error => set(d => { d.blameError = error; d.blameLoading = false; }));
        },
        clearDiff: () => { cancel('diff'); set(d => { d.activeDiff = null; d.diffError = null; d.diffLoading = false; }); },
        clearBlame: () => { cancel('blame'); set(d => { d.activeBlame = null; d.blameError = null; d.blameLoading = false; }); },
        clearAll: () => {
            for (const channel of ['log', 'diff', 'blame'] as const) cancel(channel);
            set(d => { d.gitCommits = []; d.gitLoading = false; d.gitError = null; d.gitTotal = 0; d.gitHead = null; d.activeDiff = null; d.diffLoading = false; d.diffError = null; d.activeBlame = null; d.blameLoading = false; d.blameError = null; });
        },
    })))
);

useSessionStore.subscribe((state, previous) => {
    if (state.sessionId !== previous.sessionId) useCodeInsightStore.getState().clearAll();
});
