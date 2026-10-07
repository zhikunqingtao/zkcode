/**
 * CodePathStore — 代码路径追踪状态管理
 * F40: Code Path Tracer
 */

import { create } from 'zustand';
import { AnalysisRequest, isAnalysisCancelled } from '@/api/analysisClient';
import { useSessionStore } from './sessionStore';
import { immer } from 'zustand/middleware/immer';

// ── 类型定义 ──

export interface ApiEndpointItem {
  httpMethod: string;
  path: string;
  handlerFunction: string;
  handlerClass: string;
  filePath: string;
  lineNumber: number;
  language: string;
  parameters: Array<{ name: string; type: string; annotation?: string }>;
}

export interface PathNode {
  id: string;
  name: string;
  className: string;
  filePath: string;
  lineRange: number[];
  layer: 'controller' | 'service' | 'repository' | 'database' | 'external' | 'utility';
  nodeType: string;
  annotations: string[];
  parameters: Array<{ name: string; type: string; annotation?: string }>;
  returnType: string;
}

export interface PathEdge {
  source: string;
  target: string;
  callType: string;
  parameterMapping?: Record<string, string>;
}

export interface LayerInfo {
  layer: string;
  nodeCount: number;
  description: string;
}

export interface CodePathResult {
  nodes: PathNode[];
  edges: PathEdge[];
  layers: LayerInfo[];
}

export interface CodePathState {
  // 状态
  endpoints: ApiEndpointItem[];
  pathResult: CodePathResult | null;
  selectedEndpoint: ApiEndpointItem | null;
  selectedNode: PathNode | null;
  loading: boolean;
  endpointsLoading: boolean;
  error: string | null;
  projectRoot: string;
  entryFile: string;
  entryFunction: string;
  maxDepth: number;
  /** Auto-Routing 写入的预填提示（v1.5 升级项 C Beta） */
  lastHint: Record<string, unknown> | null;

  // Actions
  setProjectRoot: (root: string) => void;
  setTraceEntry: (entry: Partial<Pick<CodePathState, 'entryFile' | 'entryFunction' | 'maxDepth'>>) => void;
  fetchEndpoints: () => Promise<void>;
  traceCodePath: (entryFile: string, entryFunction: string, maxDepth?: number) => Promise<void>;
  setSelectedEndpoint: (endpoint: ApiEndpointItem | null) => void;
  setSelectedNode: (node: PathNode | null) => void;
  applyVisualizationHint: (props: Record<string, unknown>) => void;
  reset: () => void;
}

let endpointRequest: AnalysisRequest | null = null;
let traceRequest: AnalysisRequest | null = null;
function cancelRequests() {
  endpointRequest?.cancel(); traceRequest?.cancel();
  endpointRequest = null; traceRequest = null;
}

export const useCodePathStore = create<CodePathState>()(
  immer((set, get) => ({
    endpoints: [],
    pathResult: null,
    selectedEndpoint: null,
    selectedNode: null,
    loading: false,
    endpointsLoading: false,
    error: null,
    projectRoot: '',
    entryFile: '', entryFunction: '', maxDepth: 10,
    lastHint: null,

    setProjectRoot: (root) => {
      cancelRequests();
      set(d => { d.projectRoot = root; d.endpoints = []; d.pathResult = null; d.selectedEndpoint = null; d.selectedNode = null; d.loading = false; d.endpointsLoading = false; d.error = null; });
    },

    setTraceEntry: entry => {
      traceRequest?.cancel(); traceRequest = null;
      set(d => { Object.assign(d, entry); d.pathResult = null; d.selectedNode = null; d.selectedEndpoint = null; d.loading = false; d.error = null; });
    },

    fetchEndpoints: async () => {
      endpointRequest?.cancel();
      endpointRequest = null;
      set(d => { d.endpointsLoading = true; d.error = null; d.endpoints = []; });
      let request: AnalysisRequest | null = null;
      try {
        request = new AnalysisRequest(get().projectRoot);
        endpointRequest = request;
        const result = await request.post<{ endpoints: ApiEndpointItem[] }>('/api/code-path/endpoints', {});
        if (endpointRequest !== request) return;
        if (!Array.isArray(result.endpoints)) throw new Error('分析服务返回无效端点列表');
        set(d => { d.endpoints = result.endpoints; d.endpointsLoading = false; });
      } catch (error) {
        if (request && endpointRequest !== request) return;
        set(d => { d.error = isAnalysisCancelled(error) ? null : error instanceof Error ? error.message : String(error); d.endpointsLoading = false; });
      } finally {
        if (endpointRequest === request) endpointRequest = null;
      }
    },

    traceCodePath: async (entryFile, entryFunction, maxDepth = 10) => {
      if (!entryFile.trim() || !entryFunction.trim() || !Number.isInteger(maxDepth) || maxDepth < 1 || maxDepth > 20) {
        set(d => { d.error = '请输入文件路径和函数名，深度须为 1–20 的整数'; });
        return;
      }
      traceRequest?.cancel();
      traceRequest = null;
      set(d => { d.loading = true; d.error = null; d.pathResult = null; });
      let request: AnalysisRequest | null = null;
      try {
        request = new AnalysisRequest(get().projectRoot);
        traceRequest = request;
        const result = await request.post<CodePathResult>('/api/code-path/trace', { entryFile, entryFunction, maxDepth });
        if (traceRequest !== request) return;
        if (!Array.isArray(result.nodes) || !Array.isArray(result.edges) || !Array.isArray(result.layers)) throw new Error('分析服务返回无效调用路径');
        set(d => { d.pathResult = result; d.loading = false; });
      } catch (error) {
        if (request && traceRequest !== request) return;
        set(d => { d.error = isAnalysisCancelled(error) ? null : error instanceof Error ? error.message : String(error); d.loading = false; });
      } finally {
        if (traceRequest === request) traceRequest = null;
      }
    },

    setSelectedEndpoint: (endpoint) => set(d => { d.selectedEndpoint = endpoint; }),
    setSelectedNode: (node) => set(d => { d.selectedNode = node; }),
    applyVisualizationHint: props => {
      cancelRequests();
      const text = (...keys: string[]) => keys.map(key => props[key]).find((value): value is string => typeof value === 'string' && value.trim().length > 0 && value.length <= 4096)?.trim();
      const entryFile = text('entryFile', 'filePath', 'entry_file', 'file_path');
      const entryFunction = text('entryFunction', 'functionName', 'entry_function', 'function_name');
      const root = text('projectRoot', 'project_root');
      const depth = props.maxDepth ?? props.max_depth;
      set(d => {
        d.lastHint = props; d.pathResult = null; d.selectedNode = null; d.selectedEndpoint = null;
        d.error = null; d.loading = false; d.endpointsLoading = false;
        if (entryFile) d.entryFile = entryFile;
        if (entryFunction) d.entryFunction = entryFunction;
        if (root && root !== d.projectRoot) { d.projectRoot = root; d.endpoints = []; }
        if (typeof depth === 'number' && Number.isInteger(depth) && depth >= 1 && depth <= 20) d.maxDepth = depth;
      });
    },
    reset: () => { cancelRequests(); set(d => {
      d.endpoints = [];
      d.endpointsLoading = false;
      d.pathResult = null;
      d.selectedEndpoint = null;
      d.selectedNode = null;
      d.error = null;
      d.loading = false;
      d.lastHint = null;
      d.entryFile = ''; d.entryFunction = ''; d.maxDepth = 10;
    }); },
  }))
);

const unsubscribeSession = useSessionStore.subscribe((state, previous) => {
  if (state.sessionId !== previous.sessionId) {
    useCodePathStore.getState().reset();
    useCodePathStore.setState({ projectRoot: '' });
  }
});
if (import.meta.hot) import.meta.hot.dispose(unsubscribeSession);

export function cancelPendingCodePathAnalysis(): void {
  cancelRequests();
  useCodePathStore.setState({ loading: false, endpointsLoading: false });
}
