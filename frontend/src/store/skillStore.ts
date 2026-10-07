import { create } from 'zustand';
import { useSessionStore } from './sessionStore';

export interface SkillItem {
  /** Registry identity; the displayed name may be a frontmatter alias. */
  id: string;
  name: string;
  description: string;
  source: string;
  enabled: boolean;
  userInvocable?: boolean;
}

export interface SkillDetail extends SkillItem {
  content: string;
  filePath: string;
}

interface SkillStore {
  scopeKey: string;
  projectId: string | null;
  syncScope: () => void;
  setProjectScope: (projectId: string | null) => void;
  skills: SkillItem[];
  loaded: boolean;
  loading: boolean;
  error: string | null;
  stateError: string | null;
  pending: Record<string, boolean>;
  loadSkills: (options?: { background?: boolean }) => Promise<void>;
  toggleSkill: (id: string, enabled: boolean) => Promise<void>;
}

export function findEnabledSkill(skills: SkillItem[], name: string): SkillItem | undefined {
  const normalized = name.trim().replace(/^\//, '').toLowerCase();
  // Match the backend's identity-first lookup; a disabled id must not fall through to an alias.
  const skill = skills.find(item => item.id === name)
    ?? skills.find(item => item.id.toLowerCase() === normalized)
    ?? skills.find(item => item.name.toLowerCase() === normalized);
  return skill?.enabled && skill.userInvocable !== false ? skill : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function isSkillItem(value: unknown): value is SkillItem {
  return isRecord(value)
    && typeof value.id === 'string' && value.id.trim().length > 0
    && typeof value.name === 'string'
    && typeof value.description === 'string'
    && typeof value.source === 'string'
    && typeof value.enabled === 'boolean';
}

export function isSkillDetail(value: unknown): value is SkillDetail {
  return isSkillItem(value) && isRecord(value)
    && typeof value.content === 'string' && typeof value.filePath === 'string';
}

function parseSkillList(value: unknown): { skills: SkillItem[]; stateError: string | null } {
  if (Array.isArray(value)) value = { skills: value, stateError: value.find(item => isRecord(item) && item.stateError)?.stateError ?? null };
  if (!isRecord(value) || !Array.isArray(value.skills) || !value.skills.every(isSkillItem)
      || (value.stateError != null && typeof value.stateError !== 'string')) {
    throw new Error('技能列表格式无效');
  }
  const skills: SkillItem[] = value.skills;
  if (new Set(skills.map(skill => skill.id)).size !== skills.length) throw new Error('技能列表包含重复 id');
  const stateError = typeof value.stateError === 'string' && value.stateError.trim() ? value.stateError : null;
  return { skills: stateError ? skills.map(skill => ({ ...skill, enabled: false })) : skills, stateError };
}

export const useSkillStore = create<SkillStore>((set, get) => {
  // An in-flight list from before a mutation must never restore the old switch state.
  let generation = 0;
  let requestId = 0;
  let activeLoad: { generation: number; promise: Promise<void> } | undefined;
  let scopeEpoch = 0;
  const requests = new Set<AbortController>();

  return {
    scopeKey: useSessionStore.getState().sessionId ? `session:${useSessionStore.getState().sessionId}` : 'global',
    projectId: null,
    syncScope: () => {
      const session = useSessionStore.getState().sessionId;
      const next = session ? `session:${session}` : get().projectId ? `project:${get().projectId}` : 'global';
      if (get().scopeKey === next) return;
      scopeEpoch++; generation++; requestId++;
      for (const request of requests) request.abort();
      requests.clear(); activeLoad = undefined;
      set({ scopeKey: next, skills: [], loaded: false, loading: false, error: null, stateError: null, pending: {} });
    },
    setProjectScope: projectId => { set({ projectId }); get().syncScope(); },
    skills: [],
    loaded: false,
    loading: false,
    error: null,
    stateError: null,
    pending: {},

    loadSkills: (options = {}) => {
      get().syncScope();
      if (Object.values(get().pending).some(Boolean)) return Promise.resolve();
      if (activeLoad?.generation === generation) return activeLoad.promise;
      const currentGeneration = generation;
      const currentRequest = ++requestId;
      const scope = skillRequest('/api/skills/manage');
      const controller = new AbortController(); requests.add(controller);
      if (!options.background) set({ loading: true, error: null });
      const promise = Promise.resolve().then(async () => {
        try {
          const response = await fetch(scope.url, { cache: 'no-store', headers: scope.headers, signal: AbortSignal.any([controller.signal, AbortSignal.timeout(10000)]) });
          if (!response.ok) throw new Error(`加载技能失败（HTTP ${response.status}）`);
          const data = parseSkillList(await response.json());
          if (currentRequest !== requestId || currentGeneration !== generation) return;
          set({ skills: data.skills, stateError: data.stateError, loaded: true, error: null });
        } catch (error) {
          if (currentRequest === requestId && currentGeneration === generation) {
            set({ error: isRecord(error) && error.name === 'TimeoutError'
              ? '读取技能状态超时，请重试'
              : error instanceof Error ? error.message : '无法连接服务端，请重试' });
          }
        } finally {
          requests.delete(controller);
          if (currentRequest === requestId && currentGeneration === generation) set({ loading: false });
          if (activeLoad?.generation === currentGeneration) activeLoad = undefined;
        }
      });
      activeLoad = { generation: currentGeneration, promise };
      return promise;
    },

    toggleSkill: async (id, enabled) => {
      get().syncScope();
      if (get().pending[id] === true || get().stateError !== null) return;
      if (!get().skills.some(skill => skill.id === id)) return;
      const capturedEpoch = scopeEpoch;
      const scope = skillRequest(`/api/skills/manage/${encodeURIComponent(id)}/toggle?enabled=${enabled}`);
      const controller = new AbortController(); requests.add(controller);
      generation++;
      set(state => ({ pending: { ...state.pending, [id]: true }, loading: false, error: null }));
      try {
        const response = await fetch(
          scope.url,
          { method: 'PATCH', headers: scope.headers, signal: AbortSignal.any([controller.signal, AbortSignal.timeout(10000)]) },
        );
        if (capturedEpoch !== scopeEpoch) return;
        if (!response.ok) {
          const failure: unknown = await response.json().catch(() => null);
          if (capturedEpoch !== scopeEpoch) return;
          if (response.status === 503 && isRecord(failure) && isRecord(failure.error) && failure.error.code === 'SKILL_STATE_UNAVAILABLE') {
            const stateError = typeof failure.error.message === 'string' && failure.error.message.trim()
              ? failure.error.message : '服务端技能设置不可用';
            set(state => ({ stateError, error: null, skills: state.skills.map(skill => ({ ...skill, enabled: false })) }));
            return;
          }
          const message = isRecord(failure) && isRecord(failure.error)
            && typeof failure.error.message === 'string' && failure.error.message.trim()
            ? failure.error.message : `保存技能设置失败（HTTP ${response.status}），请重试`;
          throw new Error(message);
        }
        const updated: unknown = await response.json();
        if (capturedEpoch !== scopeEpoch) return;
        if (!isSkillItem(updated) || updated.id !== id) throw new Error('保存结果无效，请刷新技能列表');
        if (get().stateError !== null) return;
        const skills = get().skills.map(skill => skill.id === id ? updated : skill);
        set({ skills });
      } catch (error) {
        if (capturedEpoch !== scopeEpoch) return;
        set({ error: isRecord(error) && error.name === 'TimeoutError'
          ? '保存技能设置超时，结果尚未确认，请刷新列表后重试'
          : error instanceof Error ? error.message : '保存技能设置失败，请检查网络后重试' });
      } finally {
        requests.delete(controller);
        if (capturedEpoch === scopeEpoch) {
          generation++;
          set(state => ({ pending: { ...state.pending, [id]: false } }));
          if (get().stateError !== null) await get().loadSkills({ background: true });
        }
      }
    },
  };
});

/** Use the saved Session identity, or an explicitly selected saved Project, never a path. */
export function skillRequest(path: string): { url: string; headers: Record<string, string>; scopeKey: string } {
  const session = useSessionStore.getState().sessionId;
  const project = useSkillStore.getState().projectId;
  return session ? { url: path, headers: { 'X-Session-Id': session }, scopeKey: `session:${session}` }
    : project ? { url: `${path}${path.includes('?') ? '&' : '?'}projectId=${encodeURIComponent(project)}`, headers: {}, scopeKey: `project:${project}` }
      : { url: path, headers: {}, scopeKey: 'global' };
}

// Clear aliases synchronously at the scope boundary, before a new input can resolve one.
const unsubscribeScope = useSessionStore.subscribe((state, previous) => {
  if (state.sessionId !== previous.sessionId) useSkillStore.getState().syncScope();
});
if (import.meta.hot) import.meta.hot.dispose(unsubscribeScope);
