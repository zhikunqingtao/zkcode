import { expect, test } from './support/production-test';
import AxeBuilder from '@axe-core/playwright';

const MODEL = 'qwen3.8-max-0902';
const ANSWER = '来自脚本 Provider 的持久化回复';

test('real backend: legacy simple preference opens the developer UI and retains diagram and math rendering', async ({ page, request }) => {
  const create = await request.post('/api/sessions', { data: { model: MODEL } });
  const { sessionId } = await create.json();
  await page.addInitScript(id => {
    window.sessionStorage.setItem('zkcode.activeSessionId', id);
    window.localStorage.setItem('zhikun.workbench.enabled', 'true');
    window.localStorage.setItem('zhikun.workbench.default-view', 'simple');
    window.localStorage.setItem(`zhikun.workbench.session-view.${id}`, 'simple');
    window.localStorage.setItem('zhikun.turn-view.v1', JSON.stringify({ state: { density: 'detailed', expandOverrides: {} }, version: 2 }));
  }, sessionId);
  await page.goto('/');
  await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText('已连接');
  await expect(page.getByRole('tablist', { name: '工作台视图' })).toHaveCount(0);
  await expect(page.getByRole('tab', { name: '简洁工作台' })).toHaveCount(0);
  await expect(page.getByRole('textbox', { name: '输入消息' })).toHaveAttribute('placeholder', /\/ 查看命令/);
  await expect(page.getByRole('tablist', { name: '显示方式' }).getByRole('tab', { name: '完整过程', exact: true })).toHaveAttribute('aria-selected', 'true');
  await page.getByRole('textbox', { name: '输入消息' }).fill('visual-regression-chart');
  await page.getByRole('button', { name: '发送消息' }).click();
  await expect(page.getByTestId('mermaid-block').locator('svg.flowchart')).toBeVisible({ timeout: 60_000 });
  await expect(page.locator('.katex').first()).toBeVisible();
  await page.screenshot({ path: test.info().outputPath('diagram-math.png'), fullPage: true });
});

test('real backend: browser chat is durable before message_complete', async ({ page, request }) => {
  const create = await request.post('/api/sessions', {
    data: { model: MODEL },
  });
  expect(create.status()).toBe(201);
  const created = await create.json() as { sessionId: string };
  expect(created.sessionId).toBeTruthy();

  await page.addInitScript(sessionId => {
    window.sessionStorage.setItem('zkcode.activeSessionId', sessionId);
  }, created.sessionId);

  const frames: Array<Record<string, unknown>> = [];
  page.on('websocket', socket => {
    if (!socket.url().includes('/ws')) return;
    socket.on('framereceived', frame => {
      try {
        const payload = JSON.parse(String(frame.payload)) as Record<string, unknown>;
        frames.push(payload);
      } catch {
        // WebSocket control frames are not application payloads.
      }
    });
  });

  await page.goto('/');
  await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText('已连接');
  await expect.poll(
    () => frames.some(frame => frame.type === 'session_restored'),
  ).toBe(true);

  const nonce = `production-${Date.now()}`;
  await page.getByRole('textbox', { name: '输入消息' }).fill(`${nonce} 请回复固定内容`);
  await page.getByRole('button', { name: '发送消息' }).click();

  await expect.poll(
    () => frames.some(frame => frame.type === 'message_complete'),
    { timeout: 60_000 },
  ).toBe(true);
  expect(frames.some(frame => frame.type === 'error')).toBe(false);
  expect(frames.some(frame => frame.type === 'stream_delta')).toBe(true);
  await expect(page.getByText(ANSWER).first()).toBeVisible();

  // Read through the public API only after observing message_complete. This
  // makes the test fail if the UI event overtakes the authoritative SQLite
  // transcript or Run terminal transition.
  const messagesResponse = await request.get(
    `/api/sessions/${encodeURIComponent(created.sessionId)}/messages?limit=20`,
  );
  expect(messagesResponse.ok()).toBe(true);
  const transcript = await messagesResponse.json() as {
    messages: Array<{ type: string; content: Array<{ type: string; text?: string }> }>;
  };
  expect(transcript.messages).toHaveLength(3);
  expect(transcript.messages[0]).toMatchObject({ type: 'system', subtype: 'task_boundary' });
  expect(frames.some(frame => frame.type === 'task_boundary')).toBe(true);
  const conversation = transcript.messages.filter(message => message.type !== 'system');
  expect(conversation).toHaveLength(2);
  expect(conversation[0].type).toBe('user');
  expect(conversation[0].content.some(block => block.text?.includes(nonce))).toBe(true);
  expect(conversation[1].type).toBe('assistant');
  expect(conversation[1].content.some(block => block.text === ANSWER)).toBe(true);

  const runsResponse = await request.get(
    `/api/runs/session/${encodeURIComponent(created.sessionId)}?limit=10`,
    { headers: { 'X-Session-Id': created.sessionId } },
  );
  expect(runsResponse.ok()).toBe(true);
  const runs = await runsResponse.json() as Array<{
    taskId: string;
    status: string;
    exitReason?: string;
  }>;
  expect(runs).toHaveLength(1);
  expect(runs[0]).toMatchObject({
    status: 'completed',
    exitReason: 'modelFinished',
  });

  const diagnosticResponse = await request.get(
    `/api/tasks/${encodeURIComponent(runs[0].taskId)}/diagnostic`,
    { headers: { 'X-Session-Id': created.sessionId } },
  );
  expect(diagnosticResponse.ok()).toBe(true);
  const diagnostic = await diagnosticResponse.json() as {
    task: { status: string };
    results: Array<{ status: string; finalMessageId?: string }>;
  };
  expect(diagnostic.task.status).toBe('succeeded');
  expect(diagnostic.results).toHaveLength(1);
  expect(diagnostic.results[0].status).toBe('complete');
  expect(diagnostic.results[0].finalMessageId).toBeTruthy();

  await page.reload();
  await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText('已连接');
  await expect(page.getByText(ANSWER).first()).toBeVisible();
  await expect(page.getByRole('main').getByText(nonce).filter({ visible: true }).first()).toBeVisible();
  await expect(page.locator('.turn-card')).toHaveCount(1);
  await page.screenshot({ path: test.info().outputPath('chat-durable.png'), fullPage: true });
});

test('real backend: failed Run remains visible after committed snapshot and reload', async ({ page, request }) => {
  const create = await request.post('/api/sessions', { data: { model: MODEL } });
  expect(create.status()).toBe(201);
  const { sessionId } = await create.json() as { sessionId: string };
  await page.addInitScript(id => {
    sessionStorage.setItem('zkcode.activeSessionId', id);
    localStorage.setItem('zhikun.turn-view.v1', JSON.stringify({ state: { density: 'balanced', expandOverrides: {} }, version: 2 }));
  }, sessionId);
  const frames: Array<Record<string, unknown>> = [];
  page.on('websocket', socket => {
    if (!socket.url().includes('/ws')) return;
    socket.on('framereceived', frame => {
      try { frames.push(JSON.parse(String(frame.payload)) as Record<string, unknown>); } catch { /* Ignore control frames. */ }
    });
  });
  await page.goto('/');
  await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText('已连接');
  // The existing local provider rejects this probe for Qwen (it requires
  // DeepSeek with explicit low effort), producing a real HTTP 422 failure.
  await page.getByRole('textbox', { name: '输入消息' }).fill('effort-wire-regression');
  await page.getByRole('button', { name: '发送消息' }).click();
  await expect.poll(() => frames.some(frame => frame.type === 'message_complete' && frame.stopReason === 'error'), { timeout: 60_000 }).toBe(true);
  const errorIndex = frames.findIndex(frame => frame.type === 'error');
  const completionIndex = frames.findIndex(frame => frame.type === 'message_complete');
  expect(errorIndex).toBeGreaterThanOrEqual(0);
  expect(completionIndex).toBeGreaterThan(errorIndex);
  type DiagnosticMessage = { type: string; subtype?: string; metadata?: { runtimeDiagnostic?: { runId: string; status: string; code?: string; message: string } } };
  const completed = frames[completionIndex].committedMessages as DiagnosticMessage[];
  const diagnostic = completed.find(message => message.subtype === 'task_boundary')?.metadata?.runtimeDiagnostic;
  expect(diagnostic?.status).toBe('failed');
  expect(diagnostic?.message).toBeTruthy();
  const assertFailedTurn = async () => {
    const turn = page.getByTestId('turn-card-0');
    await expect(turn.getByText(diagnostic!.message, { exact: true })).toBeVisible();
    await expect(turn.getByRole('img', { name: '失败', exact: true })).toBeVisible();
    await expect(turn.getByRole('img', { name: '已完成', exact: true })).toHaveCount(0);
  };
  await assertFailedTurn();
  const persisted = await request.get(`/api/sessions/${sessionId}/messages?limit=20`);
  expect(persisted.ok()).toBe(true);
  expect((await persisted.json()).messages.find((message: DiagnosticMessage) => message.subtype === 'task_boundary')?.metadata.runtimeDiagnostic).toEqual(diagnostic);
  await page.reload();
  await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText('已连接');
  await assertFailedTurn();
  await expect(page.getByTitle(/当前会话成本.*费用未知/)).toContainText('费用未知');
  await page.screenshot({ path: test.info().outputPath('failed-run-durable.png'), fullPage: true });
});

test('real backend: settings, Skill management, and revisioned memory use native contracts', async ({ page, request }) => {
  await page.goto('/');
  await page.getByRole('button', { name: '外观设置', exact: true }).click();
  const settings = page.getByRole('dialog', { name: '设置', exact: true });
  await expect(settings.getByRole('tab', { name: 'API Keys' })).toBeVisible();
  await expect(settings.getByRole('checkbox', { name: /语音识别使用最近/ })).not.toBeChecked();
  await settings.getByRole('button', { name: '跟随系统', exact: true }).click();
  await expect(settings.getByLabel('当前会话模型')).toBeVisible();
  await expect(settings.getByLabel('新会话默认模型')).toBeVisible();
  await settings.getByRole('button', { name: '关闭设置' }).click();
  await expect(page.getByRole('button', { name: '外观设置', exact: true })).toContainText('跟随系统');

  const skillResponse = page.waitForResponse(response => response.url().includes('/api/skills/manage') && response.request().method() === 'GET');
  await page.getByRole('button', { name: 'Skill 管理', exact: true }).click();
  const skills = await skillResponse;
  expect(skills.ok()).toBe(true);
  expect(Array.isArray(await skills.json())).toBe(true);
  await expect(page.getByRole('dialog', { name: 'Skill 管理' })).toBeVisible();
  await expect(page.getByRole('button', { name: '刷新 Skill 列表' })).toBeEnabled();
  await page.getByRole('button', { name: '关闭 Skill 管理' }).click();

  const initialResponse = await request.get('/api/memory/document?scope=global');
  expect(initialResponse.ok()).toBe(true);
  const initial = await initialResponse.json() as { revision: number };
  await page.getByRole('button', { name: '记忆', exact: true }).click();
  await page.getByRole('button', { name: '新增条目' }).click();
  const content = `SQLite memory ${Date.now()}`;
  await page.getByRole('textbox', { name: '记忆条目 1 内容' }).fill(content);
  const savedResponse = page.waitForResponse(response => response.url().endsWith('/api/memory/document/entries') && response.request().method() === 'PUT');
  await page.getByRole('button', { name: '保存记忆' }).filter({ visible: true }).click();
  const saved = await savedResponse;
  expect(saved.ok(), await saved.text()).toBe(true);
  await expect(page.getByText('已保存', { exact: true })).toBeVisible();
  const stale = await request.put('/api/memory/document', { data: { scope: 'global', expectedRevision: initial.revision, content: 'stale overwrite' } });
  expect(stale.status()).toBe(409);
  await page.getByRole('button', { name: '关闭记忆页面' }).click();
  await page.reload();
  await page.getByRole('button', { name: '记忆', exact: true }).click();
  await expect(page.getByRole('textbox', { name: '记忆条目 1 内容' })).toHaveValue(content);
});

for (const [mergeModel, pricingStatus] of [[MODEL, 'known'], ['qwen3.8-flash', 'unknown']] as const) {
test(`real backend: merge UI preserves ${pricingStatus} pricing, durable handoff and primary permission`, async ({ page, request }) => {
  for (let index = 0; index < 2; index++) {
    const response = await request.post('/api/sessions', { data: { model: MODEL, permissionMode: 'DONT_ASK' } });
    expect(response.status()).toBe(201);
  }
  await page.goto('/');
  await page.getByRole('button', { name: '合并为新会话', exact: true }).first().click();
  const dialog = page.getByRole('dialog', { name: '合并为新会话' });
  await expect(dialog.getByText('新会话继承主会话的权限模式', { exact: true })).toBeVisible();
  await dialog.getByRole('group', { name: '选择来源会话' }).getByRole('button').first().click();
  const title = `SQLite handoff integration ${pricingStatus}`;
  await dialog.getByLabel('新会话标题').fill(title);
  await dialog.getByLabel('目标模型').selectOption(mergeModel);
  const creating = page.waitForResponse(response => response.url().endsWith('/api/sessions/merge') && response.request().method() === 'POST');
  await dialog.getByRole('button', { name: '开始合并', exact: true }).click();
  const response = await creating;
  expect(response.status()).toBe(202);
  const operation = await response.json() as { operationId: string; targetSessionId: string };
  const idempotencyKey = response.request().headers()['idempotency-key'];
  const replay = await request.post('/api/sessions/merge', {
    headers: { 'Idempotency-Key': idempotencyKey, Origin: new URL(page.url()).origin },
    data: response.request().postDataJSON(),
  });
  expect(replay.status()).toBe(202);
  expect((await replay.json()).operationId).toBe(operation.operationId);
  await expect.poll(async () => {
    const progress = await request.get(`/api/session-merges/${operation.operationId}`);
    return (await progress.json()).status;
  }, { timeout: 60_000 }).toBe('completed');
  const result = await (await request.get(`/api/session-merges/${operation.operationId}`)).json();
  expect(result.targetAvailable).toBe(true);
  expect(result.progress.completedUnits).toBe(result.progress.knownUnits);
  expect(result.result.handoffStorage).toBe('sqlite');
  expect(result.usage.usageComplete).toBe(true);
  expect(result.usage.tokens).toBeGreaterThan(0);
  expect(result.usage.pricingStatus).toBe(pricingStatus);
  if (pricingStatus === 'known') expect(result.usage.costNanosUsd).toBeGreaterThan(0);
  else expect(result.usage.costNanosUsd).toBe(0);
  const targetListing = await (await request.get(`/api/sessions?q=${encodeURIComponent(title)}`)).json();
  expect(targetListing.sessions.find((session: { id: string }) => session.id === operation.targetSessionId).permissionMode).toBe('DONT_ASK');
  await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText(operation.targetSessionId.slice(0, 8));
  await page.reload();
  await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText(operation.targetSessionId.slice(0, 8));
  const transcript = await (await request.get(`/api/sessions/${operation.targetSessionId}/messages?limit=20`)).json();
  expect(transcript.messages.some((message: { content: unknown }) => JSON.stringify(message.content).includes('历史参考'))).toBe(true);
  await expect(page.getByText('合并交接已就绪')).toBeVisible();
  await page.getByText('查看交接摘要与来源记录').click();
  await expect(page.getByRole('region', { name: '合并交接' })).toContainText('历史参考');
  await page.screenshot({ path: test.info().outputPath('merge-handoff.png'), fullPage: true });
  await page.getByRole('button', { name: '合并结果', exact: true }).click();
  const mergeUsage = page.getByRole('dialog', { name: '合并为新会话' }).getByText(/合并用量：/);
  if (pricingStatus === 'unknown') {
    await expect(mergeUsage).toContainText('费用未知');
    await expect(mergeUsage).not.toContainText('$0.000000');
  } else {
    await expect(mergeUsage).toContainText(`$${(result.usage.costNanosUsd / 1_000_000_000).toFixed(6)}`);
  }
  await page.screenshot({ path: test.info().outputPath('merge-pricing.png'), fullPage: true });
});
}

test('real backend: persisted editor chords, Vim and session execution controls are functional', async ({ page, request }) => {
  const effortModel = 'deepseek-flash';
  const create = await request.post('/api/sessions', { data: { model: effortModel } });
  expect(create.status()).toBe(201);
  const { sessionId } = await create.json();
  const editorPreferences = { vimEnabled: true, keybindings: { 'chat:commandPalette': 'ctrl+k ctrl+g' } };
  const saved = await request.put('/api/config', { data: { editorPreferences } });
  expect(saved.ok(), await saved.text()).toBe(true);
  await page.addInitScript(id => {
    sessionStorage.setItem('zkcode.activeSessionId', id);
  }, sessionId);
  await page.goto('/');
  await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText('已连接');
  const composer = page.getByRole('textbox', { name: '输入消息' });
  await expect(page.getByText('VIM · INSERT', { exact: true })).toBeVisible();
  await composer.fill('hello world');
  await composer.press('Home');
  await composer.press('Escape');
  await composer.press('d'); await composer.press('w');
  await expect(composer).toHaveValue('world');
  await composer.press('u'); await expect(composer).toHaveValue('hello world');
  await composer.press('Control+k');
  await expect(page.getByText(/等待和弦下一键/)).toBeVisible();
  await composer.press('Control+g');
  await expect(page.getByPlaceholder('Type a command...')).toBeVisible();
  await page.getByPlaceholder('Type a command...').press('Escape');
  await composer.fill('/vim off');
  await page.getByRole('button', { name: '发送消息' }).click();
  await expect.poll(async () => (await (await request.get('/api/config')).json()).editorPreferences.vimEnabled).toBe(false);
  await expect(page.getByText(/VIM ·/)).toHaveCount(0);

  await page.getByRole('button', { name: '外观设置', exact: true }).click();
  const settings = page.getByRole('dialog', { name: '设置', exact: true });
  const effort = settings.getByRole('combobox', { name: '当前会话推理强度' });
  await expect(effort).toBeEnabled();
  const headers = { 'X-Session-Id': sessionId };
  const before = await (await request.get(`/api/sessions/${sessionId}/execution-preferences`, { headers })).json();
  const unsupportedCreate = await request.post('/api/sessions', { data: { model: MODEL } });
  expect(unsupportedCreate.status()).toBe(201);
  const unsupportedId = (await unsupportedCreate.json()).sessionId;
  const unsupportedHeaders = { 'X-Session-Id': unsupportedId };
  const unsupported = await (await request.get(`/api/sessions/${unsupportedId}/execution-preferences`, { headers: unsupportedHeaders })).json();
  expect(unsupported.supportedEfforts).toEqual([]);
  const rejected = await request.patch(`/api/sessions/${unsupportedId}/execution-preferences`, { headers: unsupportedHeaders, data: { revision: unsupported.revision, effort: 'low' } });
  expect(rejected.status()).toBe(400);
  expect((await rejected.json()).code).toBe('SESSION_EXECUTION_OPTIONS_UNSUPPORTED');
  expect((await (await request.get(`/api/sessions/${unsupportedId}/execution-preferences`, { headers: unsupportedHeaders })).json()).effort).toBe('auto');
  expect(before.fast).toBe(false);
  expect(before.effort).toBe('auto');
  expect(before.supportedEfforts).toContain('low');
  await effort.selectOption('low');
  await expect.poll(async () => (await (await request.get(`/api/sessions/${sessionId}/execution-preferences`, { headers })).json()).effort).toBe('low');
  await settings.getByRole('button', { name: '关闭设置' }).click();
  await composer.fill('effort-wire-regression');
  await page.getByRole('button', { name: '发送消息' }).click();
  await expect(page.getByText(ANSWER, { exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: '发送消息' })).toBeVisible();
  await composer.fill('/effort auto');
  await page.getByRole('button', { name: '发送消息' }).click();
  await expect.poll(async () => (await (await request.get(`/api/sessions/${sessionId}/execution-preferences`, { headers })).json()).effort).toBe('auto');
  const session = await (await request.get(`/api/sessions/${sessionId}`)).json();
  expect(session.model).toBe(effortModel);
  const another = await request.post('/api/sessions', { data: { model: effortModel } });
  const { sessionId: otherId } = await another.json();
  const untouched = await (await request.get(`/api/sessions/${otherId}/execution-preferences`, { headers: { 'X-Session-Id': otherId } })).json();
  expect(untouched.effort).toBe('auto'); expect(untouched.fast).toBe(false);
  const reset = await request.put('/api/config', { data: { editorPreferences: { vimEnabled: false, keybindings: {} } } });
  expect(reset.ok()).toBe(true);
});

test('real backend: browser replay panel is reachable and uses exact session access', async ({ page, request }) => {
  const created = await request.post('/api/sessions', { data: { model: MODEL } });
  expect(created.status()).toBe(201);
  const { sessionId } = await created.json() as { sessionId: string };
  await page.addInitScript(id => {
    window.sessionStorage.setItem('zkcode.activeSessionId', id);
  }, sessionId);
  await page.goto('/');
  await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText('已连接');
  const replay = page.waitForResponse(response => response.url().endsWith(`/api/browser/replay/${sessionId}`));
  await page.getByRole('combobox', { name: '侧栏面板' }).selectOption('browser');
  const result = await replay;
  expect(result.request().headers()['x-session-id']).toBe(sessionId);
  expect(result.status()).toBe(404);
  expect(await result.json()).toMatchObject({ code: 'REPLAY_NOT_FOUND' });
  await expect(page.getByRole('region', { name: '浏览器快照时间线' })).toContainText('暂无快照');
  const denied = await request.get(`/api/browser/replay/${sessionId}`, { headers: { 'X-Session-Id': 'foreign-session' } });
  expect(denied.status()).toBe(404);
  expect(await denied.json()).toMatchObject({ code: 'SESSION_NOT_FOUND' });
});


test('real backend: session navigation preserves drafts and bind needs no duplicate REST refresh', async ({ page, request }) => {
  const ids: string[] = [];
  for (let index = 0; index < 2; index++) {
    const created = await request.post('/api/sessions', { data: { model: MODEL } });
    expect(created.status()).toBe(201);
    ids.push((await created.json()).sessionId);
  }
  const interactionReads: string[] = [];
  page.on('request', item => { if (item.url().includes('/api/interactions/pending')) interactionReads.push(item.url()); });
  await page.addInitScript(id => sessionStorage.setItem('zkcode.activeSessionId', id), ids[0]);
  await page.goto('/');
  const input = page.getByRole('textbox', { name: '输入消息' });
  await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText(ids[0].slice(0, 8));
  await input.fill('desktop draft A');
  await page.locator(`[title^="${ids[1]} ·"]`).click();
  await expect(page.getByRole('button', { name: '查看会话详情' })).toContainText(ids[1].slice(0, 8));
  await expect(input).toHaveValue('');
  await input.fill('desktop draft B');
  await page.locator(`[title^="${ids[0]} ·"]`).click();
  await expect(input).toHaveValue('desktop draft A');
  expect(interactionReads).toEqual([]);
  const before = await (await request.get(`/api/sessions/${ids[0]}/messages?limit=20`)).json();
  expect(before.messages).toEqual([]);
});

test('real backend: main page and MCP controls pass WCAG in light and dark themes', async ({ page, request }) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  for (const mode of ['light', 'dark']) {
    const configured = await request.put('/api/config', { data: { theme: mode } });
    expect(configured.ok()).toBe(true);
    await page.goto('/');
    await expect(page.locator('html')).toHaveClass(new RegExp(mode));
    await expect(page.getByRole('textbox', { name: '输入消息' })).toBeVisible();
    const main = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa']).analyze();
    expect(main.violations).toEqual([]);
    await page.getByRole('button', { name: 'MCP 管理', exact: true }).click();
    await expect(page.getByRole('heading', { name: 'MCP 管理', exact: true })).toBeVisible();
    const mcp = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa']).analyze();
    expect(mcp.violations).toEqual([]);
    await page.getByRole('dialog', { name: 'MCP 管理', exact: true }).getByRole('button', { name: '关闭', exact: true }).click();
  }
});
