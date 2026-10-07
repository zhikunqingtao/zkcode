/**
 * ComplexityStore — 代码复杂度数据管理
 * 管理代码复杂度分析结果、钻取导航、过滤状态
 */

import { create } from 'zustand';
import { AnalysisRequest, isAnalysisCancelled } from '@/api/analysisClient';
import { useSessionStore } from './sessionStore';
import { immer } from 'zustand/middleware/immer';
import { subscribeWithSelector } from 'zustand/middleware';

// ── 类型定义（基于 Python API 响应） ──

export interface ComplexityNode {
  name: string;
  type: 'project' | 'directory' | 'file' | 'class' | 'method';
  loc: number;
  cc: number;
  mi: number;
  risk_level: 'A' | 'B' | 'C' | 'D' | 'E';
  children?: ComplexityNode[];
  file_path?: string;
  language?: string;
}

export interface ComplexityStats {
  total_files: number;
  avg_cc: number;
  high_risk_count: number;
  analysis_time_ms: number;
}

interface ComplexityApiResponse {
  success: boolean;
  data: {
    root: ComplexityNode;
    stats: ComplexityStats;
    cached: boolean;
    truncated: boolean;
    analysis_kind: 'heuristic';
    is_verification_evidence: false;
  } | null;
  error_message?: string;
  elapsed_ms?: number;
}

// ── Store 状态 ──

export interface ComplexityState {
  complexityTree: ComplexityNode | null;
  stats: ComplexityStats | null;
  isLoading: boolean;
  error: string | null;
  cached: boolean;
  truncated: boolean;
  lastRequest: { projectRoot: string; targetPath?: string; languages?: string[] } | null;

  // 钻取导航状态
  currentDrillPath: ComplexityNode[];   // 面包屑路径栈
  currentNode: ComplexityNode | null;   // 当前显示的节点

  // 过滤状态
  languageFilter: string | null;
  riskLevelFilter: string[] | null;     // e.g., ['C', 'D', 'E']

  /** Auto-Routing 写入的预填提示（v1.5 升级项 C Beta） */
  lastHint: Record<string, unknown> | null;

  // Actions
  fetchComplexity: (projectRoot: string, targetPath?: string, languages?: string[]) => Promise<void>;
  drillDown: (node: ComplexityNode) => void;
  drillUp: (index?: number) => void;    // 面包屑导航，index 可跳到指定层
  setLanguageFilter: (language: string | null) => void;
  setRiskLevelFilter: (levels: string[] | null) => void;
  applyVisualizationHint: (props: Record<string, unknown>) => void;
  reset: () => void;
}

let activeRequest: AnalysisRequest | null = null;

export const useComplexityStore = create<ComplexityState>()(
  subscribeWithSelector(immer((set, _get) => ({
    complexityTree: null,
    stats: null,
    isLoading: false,
    error: null,
    cached: false,
    truncated: false,
    lastRequest: null,

    currentDrillPath: [],
    currentNode: null,

    languageFilter: null,
    riskLevelFilter: null,
    lastHint: null,

    fetchComplexity: async (projectRoot, targetPath, languages) => {
      cancelPendingComplexityAnalysis();
      set(d => { d.isLoading = true; d.error = null; d.complexityTree = null; d.currentNode = null; d.stats = null; d.currentDrillPath = []; d.lastRequest = { projectRoot, targetPath, languages }; });
      let request: AnalysisRequest | null = null;
      try {
        request = new AnalysisRequest(projectRoot);
        activeRequest = request;
        const json = await request.post<ComplexityApiResponse>('/api/code-quality/complexity', { targetPath, languages });
        if (activeRequest !== request) return;
        const data = json.data;
        if (!json.success || !data?.root || !data.stats || data.analysis_kind !== 'heuristic' || data.is_verification_evidence !== false || typeof data.truncated !== 'boolean') throw new Error('复杂度服务返回无效的辅助分析结果');
        set(d => {
          d.complexityTree = data.root; d.stats = data.stats; d.cached = data.cached;
          d.truncated = data.truncated; d.isLoading = false;
          d.currentNode = data.root; d.currentDrillPath = [data.root];
        });
      } catch (error) {
        if (request && activeRequest !== request) return;
        set(d => { d.error = isAnalysisCancelled(error) ? null : error instanceof Error ? error.message : String(error); d.isLoading = false; });
      } finally {
        if (activeRequest === request) activeRequest = null;
      }
    },

    drillDown: (node) => {
      if (!node.children || node.children.length === 0) return;
      set(d => {
        d.currentDrillPath.push(node);
        d.currentNode = node;
      });
    },

    drillUp: (index) => {
      set(d => {
        if (index !== undefined && index >= 0 && index < d.currentDrillPath.length) {
          d.currentDrillPath = d.currentDrillPath.slice(0, index + 1);
          d.currentNode = d.currentDrillPath[index];
        } else if (d.currentDrillPath.length > 1) {
          d.currentDrillPath.pop();
          d.currentNode = d.currentDrillPath[d.currentDrillPath.length - 1];
        }
      });
    },

    setLanguageFilter: (language) => {
      set(d => { d.languageFilter = language; });
    },

    setRiskLevelFilter: (levels) => {
      set(d => { d.riskLevelFilter = levels; });
    },

    applyVisualizationHint: (props) => {
      set(d => { d.lastHint = props ?? null; });
    },

    reset: () => {
      cancelPendingComplexityAnalysis();
      set(d => {
        d.complexityTree = null;
        d.stats = null;
        d.isLoading = false;
        d.error = null;
        d.cached = false;
        d.truncated = false;
        d.lastRequest = null;
        d.currentDrillPath = [];
        d.currentNode = null;
        d.languageFilter = null;
        d.riskLevelFilter = null;
        d.lastHint = null;
      });
    },
  })))
);

export function cancelPendingComplexityAnalysis(): void {
  activeRequest?.cancel(); activeRequest = null;
  useComplexityStore.setState({ isLoading: false });
}
useSessionStore.subscribe((state, previous) => {
  if (state.sessionId !== previous.sessionId) useComplexityStore.getState().reset();
});
