import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useApiContractStore } from '../apiContractStore';

const response = (title: string, warnings: string[] = []) => new Response(JSON.stringify({ openapi: '3.1.0', info: { title, version: '1' }, paths: {}, warnings }));

describe('native OpenAPI sources', () => {
  beforeEach(() => useApiContractStore.getState().reset());
  afterEach(() => vi.unstubAllGlobals());

  it('shows native backend and partial-source warnings', async () => {
    const fetch = vi.fn().mockResolvedValue(response('Rust', ['Python unavailable']));
    vi.stubGlobal('fetch', fetch);
    await useApiContractStore.getState().fetchOpenApiSpec('backend');
    expect(fetch.mock.calls[0][0]).toBe('/api/analysis/openapi/backend');
    expect(useApiContractStore.getState().openApiSpec?.info.title).toBe('Rust');
    expect(useApiContractStore.getState().warnings).toEqual(['Python unavailable']);
  });

  it('aborts switched sources and ignores late or post-reset results', async () => {
    const complete: ((value: Response) => void)[] = [];
    const fetch = vi.fn().mockImplementation(() => new Promise<Response>(resolve => complete.push(resolve)));
    vi.stubGlobal('fetch', fetch);
    const old = useApiContractStore.getState().fetchOpenApiSpec('python');
    const current = useApiContractStore.getState().fetchOpenApiSpec('backend');
    expect(fetch.mock.calls[0][1].signal.aborted).toBe(true);
    complete[1](response('Rust')); await current;
    complete[0](response('Python')); await old;
    expect(useApiContractStore.getState().openApiSpec?.info.title).toBe('Rust');
    const pending = useApiContractStore.getState().fetchOpenApiSpec();
    useApiContractStore.getState().reset();
    complete[2](response('merged')); await pending;
    expect(useApiContractStore.getState().openApiSpec).toBeNull();
    expect(useApiContractStore.getState().isLoading).toBe(false);
  });
});
