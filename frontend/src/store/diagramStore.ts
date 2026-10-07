/**
 * DiagramStore — 代码→图表自动生成状态管理
 * F35: Code Diagram Generator
 */

import { create } from 'zustand';
import { AnalysisRequest, isAnalysisCancelled } from '@/api/analysisClient';
import { useSessionStore } from './sessionStore';
import { immer } from 'zustand/middleware/immer';

// ── 类型定义 ──

export interface DiagramGenerationResult {
  diagramType: string;
  mermaidSyntax: string;
  confidenceScore: number;
  metadata: {
    nodesCount: number;
    edgesCount: number;
    languagesAnalyzed: string[];
    analysisTimeMs: number;
  };
  warnings: string[];
}

export interface DiagramState {
  // 状态
  diagramType: 'sequence' | 'flowchart';
  target: string;
  projectRoot: string;
  depth: number;
  result: DiagramGenerationResult | null;
  loading: boolean;
  error: string | null;

  // 编辑模式
  editedMermaidSyntax: string | null;

  // Actions
  setDiagramType: (type: 'sequence' | 'flowchart') => void;
  setTarget: (target: string) => void;
  setProjectRoot: (root: string) => void;
  setDepth: (depth: number) => void;
  generateDiagram: () => Promise<void>;
  clearDiagram: () => void;
  updateMermaidSyntax: (syntax: string) => void;
}

let activeRequest: AnalysisRequest | null = null;
function cancelRequest() {
  activeRequest?.cancel();
  activeRequest = null;
}

export const useDiagramStore = create<DiagramState>()(
  immer((set, get) => ({
    diagramType: 'sequence',
    target: '',
    projectRoot: '.',
    depth: 3,
    result: null,
    loading: false,
    error: null,
    editedMermaidSyntax: null,

    setDiagramType: (type) => { cancelRequest(); set(d => { d.diagramType = type; d.result = null; d.loading = false; d.error = null; }); },
    setTarget: (target) => { cancelRequest(); set(d => { d.target = target; d.result = null; d.loading = false; d.error = null; }); },
    setProjectRoot: (root) => { cancelRequest(); set(d => { d.projectRoot = root; d.result = null; d.loading = false; d.error = null; }); },
    setDepth: (depth) => { cancelRequest(); set(d => { d.depth = depth; d.result = null; d.loading = false; d.error = null; }); },

    generateDiagram: async () => {
      const { diagramType, target, projectRoot, depth } = get();
      if (!target.trim()) {
        set(d => { d.error = '请输入目标路径或方法签名'; });
        return;
      }
      cancelRequest();
      set(d => { d.loading = true; d.error = null; d.result = null; d.editedMermaidSyntax = null; });
      let request: AnalysisRequest | null = null;
      try {
        request = new AnalysisRequest(projectRoot);
        activeRequest = request;
        const result = await request.post<DiagramGenerationResult>('/api/code-diagrams/generate', { diagramType, target, depth });
        if (activeRequest !== request) return;
        if (typeof result.mermaidSyntax !== 'string' || !result.mermaidSyntax.trim() || !Number.isFinite(result.confidenceScore) || !result.metadata) {
          throw new Error('分析服务返回无效图表');
        }
        set(d => { d.result = result; d.loading = false; });
      } catch (error) {
        if (request && activeRequest !== request) return;
        set(d => { d.error = isAnalysisCancelled(error) ? null : error instanceof Error ? error.message : String(error); d.loading = false; });
      } finally {
        if (activeRequest === request) activeRequest = null;
      }
    },

    clearDiagram: () => {
      cancelRequest();
      set(d => { d.result = null; d.error = null; d.loading = false; d.editedMermaidSyntax = null; });
    },

    updateMermaidSyntax: (syntax) => set(d => {
      d.editedMermaidSyntax = syntax;
    }),
  }))
);

const unsubscribeSession = useSessionStore.subscribe((state, previous) => {
  if (state.sessionId !== previous.sessionId) {
    useDiagramStore.getState().clearDiagram();
    useDiagramStore.setState({ projectRoot: '.' });
  }
});
if (import.meta.hot) import.meta.hot.dispose(unsubscribeSession);

export function cancelPendingDiagramAnalysis(): void {
  cancelRequest();
  useDiagramStore.setState({ loading: false });
}
