import { expect, test } from '@playwright/test';

test('React analysis panels reach real Rust and Python parsers using session authority', async ({ page, request }) => {
    const created = await request.post('/api/sessions', { data: {} });
    expect(created.status()).toBe(201);
    const { sessionId } = await created.json();
    await page.addInitScript(id => {
        sessionStorage.setItem('zkcode.activeSessionId', id);
    }, sessionId);
    await page.goto('/');
    await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText('已连接');
    await page.getByRole('combobox', { name: '侧栏面板' }).selectOption('diagram');
    await page.getByRole('button', { name: '流程图', exact: true }).click();
    await page.getByRole('textbox', { name: '方法签名' }).fill('get_user');
    const generated = page.waitForResponse(response => response.url().endsWith('/api/code-diagrams/generate'));
    await page.getByRole('button', { name: '生成图表', exact: true }).click();
    const diagramResponse = await generated;
    expect(diagramResponse.ok(), await diagramResponse.text()).toBe(true);
    const diagram = await diagramResponse.json();
    expect(diagram.metadata.nodesCount).toBeGreaterThan(2);
    expect(diagram.mermaidSyntax).toContain('flowchart');
    await expect(page.locator('.app-sidebar svg.flowchart')).toBeVisible();

    await page.getByRole('combobox', { name: '侧栏面板' }).selectOption('code-path');
    const scanned = page.waitForResponse(response => response.url().endsWith('/api/code-path/endpoints'));
    await page.getByRole('button', { name: '扫描', exact: true }).click();
    const endpoints = await (await scanned).json();
    expect(endpoints.endpoints[0]).toMatchObject({ httpMethod: 'GET', path: '/users', handlerFunction: 'get_user' });
    const traced = page.waitForResponse(response => response.url().endsWith('/api/code-path/trace'));
    await page.getByRole('button', { name: /\/users.*get_user/ }).click();
    const traceResponse = await traced;
    expect(traceResponse.ok(), await traceResponse.text()).toBe(true);
    const graph = await traceResponse.json();
    expect(graph.nodes.some((node: { name: string }) => node.name === 'fetch')).toBe(true);
    await expect(page.locator('.code-path-tracer .react-flow__node')).toHaveCount(graph.nodes.length);
    await page.screenshot({ path: test.info().outputPath('real-code-path.png'), fullPage: true });
    await page.getByRole('combobox', { name: '侧栏面板' }).selectOption('impact');
    await page.getByRole('textbox', { name: '影响分析文件路径' }).fill('api.py');
    await page.getByRole('textbox', { name: '影响分析变更行号' }).fill('10, 11');
    const impacted = page.waitForResponse(response => response.url().endsWith('/api/analysis/change-impact'));
    await page.getByRole('button', { name: '分析影响', exact: true }).click();
    const impactResponse = await impacted;
    expect(impactResponse.ok(), await impactResponse.text()).toBe(true);
    const impact = await impactResponse.json();
    expect(impact.data).toMatchObject({ analysis_kind: 'advisory', is_verification_evidence: false });
    expect(impact.data.impact_nodes.length).toBeGreaterThan(0);
    expect(impact.data.impact_nodes.some((node: { name: string }) => node.name.includes('get_user'))).toBe(true);
    await expect(page.getByText('辅助分析，非验证结果；仍需运行实际测试。')).toBeVisible();
    await expect(page.locator('.impact-graph .react-flow__node')).toHaveCount(impact.data.impact_nodes.length + 1);
    await page.screenshot({ path: test.info().outputPath('real-change-impact.png'), fullPage: true });

    const leaving = page.waitForEvent('dialog');
    await page.evaluate(() => { setTimeout(() => location.reload(), 0); });
    const confirmation = await leaving;
    expect(confirmation.type()).toBe('beforeunload');
    await confirmation.dismiss();
    await expect(page.locator('.impact-graph .react-flow__node')).toHaveCount(impact.data.impact_nodes.length + 1);
});

test('complexity and Git panels use real authorized parsers and native Git', async ({ page, request }) => {
    const created = await request.post('/api/sessions', { data: {} });
    expect(created.status()).toBe(201);
    const { sessionId } = await created.json();
    await page.addInitScript(id => {
        sessionStorage.setItem('zkcode.activeSessionId', id);
    }, sessionId);
    await page.goto('/');
    await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText('已连接');
    await page.getByRole('combobox', { name: '侧栏面板' }).selectOption('complexity');
    await page.getByRole('textbox', { name: '复杂度分析目标路径' }).fill('api.py');
    const analyzed = page.waitForResponse(response => response.url().endsWith('/api/code-quality/complexity'));
    await page.getByRole('button', { name: '分析复杂度', exact: true }).click();
    const result = await analyzed;
    expect(result.ok(), await result.text()).toBe(true);
    const metrics = await result.json();
    expect(metrics.data).toMatchObject({ analysis_kind: 'heuristic', is_verification_evidence: false, truncated: false });
    expect(metrics.data.stats.total_files).toBe(1);
    expect(metrics.data.root.cc).toBeGreaterThan(0);
    await expect(page.getByText('启发式复杂度指标，不是测试或验证结果。支持 Python、Java、TypeScript、JavaScript。')).toBeVisible();
    await expect(page.locator('.app-sidebar').getByText('1 文件', { exact: true })).toBeVisible();
    const history = page.waitForResponse(response => response.url().endsWith('/api/git/log'));
    await page.getByRole('combobox', { name: '侧栏面板' }).selectOption('git');
    const log = await history;
    expect(log.ok(), await log.text()).toBe(true);
    const data = (await log.json()).data;
    expect(data.total).toBe(1);
    expect(data.commits[0].sha).toBe(data.head);
    await expect(page.getByText('feat: native analysis fixture', { exact: true })).toBeVisible();
    const changed = page.waitForResponse(response => response.url().endsWith('/api/git/diff'));
    await page.getByTitle('查看 Diff', { exact: true }).click();
    const diff = await changed;
    expect(diff.ok(), await diff.text()).toBe(true);
    expect((await diff.json()).data.detailed).toContain('+def get_user():');
    await page.getByText('feat: native analysis fixture', { exact: true }).click();
    const attributed = page.waitForResponse(response => response.url().endsWith('/api/git/blame'));
    await page.getByRole('button', { name: 'Blame', exact: true }).click();
    const blame = await attributed;
    expect(blame.ok(), await blame.text()).toBe(true);
    const provenance = (await blame.json()).data;
    expect(provenance.file_path).toBe('api.py');
    expect(provenance.lines.some((line: { content: string; sha: string }) => line.content === 'def get_user():' && line.sha === data.head)).toBe(true);
    await expect(page.locator('.app-sidebar').getByText('def get_user():', { exact: true })).toBeVisible();
});

test('gateway authorizes aliases, honors pre-start cancellation, and exposes actual native docs', async ({ request }) => {
    const created = await request.post('/api/sessions', { data: {} });
    const { sessionId } = await created.json();
    const headers = { 'X-Session-Id': sessionId };
    const forbidden = await request.post('/api/analysis/api-endpoints', { headers, data: { sessionId, project_root: '/' } });
    expect(forbidden.status()).toBe(403);
    const unbound = await request.post('/api/code-path/endpoints', { data: { projectRoot: '.' } });
    expect(unbound.status()).toBe(400);
    const requestId = crypto.randomUUID();
    const cancelled = await request.post('/api/code-analysis/cancel', { headers, data: { sessionId, requestId } });
    expect(cancelled.ok(), await cancelled.text()).toBe(true);
    const replay = await request.post('/api/code-path/endpoints', { headers, data: { sessionId, requestId } });
    expect(replay.status()).toBe(409);
    const merged = await request.get('/api/analysis/openapi/merged');
    expect(merged.ok()).toBe(true);
    const spec = await merged.json();
    expect(spec.paths['/api/sessions']).toBeTruthy();
    expect(spec.paths['/api/code-diagrams/generate']).toBeTruthy();
    expect(spec.paths['/api/analysis/api-endpoints'].post['x-zk-source']).toBe('python');
    expect(spec.paths['/api/analysis/change-impact'].post.operationId).toBe('change_impact');
    expect(spec.warnings).toEqual([
        'Rust owns post /api/analysis/change-impact; the Python service variant is available in the Python tab.',
        'Rust owns post /api/code-quality/complexity; the Python service variant is available in the Python tab.',
        'Rust owns post /api/git/blame; the Python service variant is available in the Python tab.',
        'Rust owns post /api/git/diff; the Python service variant is available in the Python tab.',
        'Rust owns post /api/git/log; the Python service variant is available in the Python tab.',
        'Rust owns get /api/health; the Python service variant is available in the Python tab.',
    ]);
});
