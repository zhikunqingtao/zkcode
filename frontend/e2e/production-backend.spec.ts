import { expect, test } from '@playwright/test';

const MODEL = 'qwen3.8-max-0902';
const ANSWER = '来自脚本 Provider 的持久化回复';

test('real backend: browser chat is durable before message_complete', async ({ page, request }) => {
  const create = await request.post('/api/sessions', {
    data: { model: MODEL },
  });
  expect(create.status()).toBe(201);
  const created = await create.json() as { sessionId: string };
  expect(created.sessionId).toBeTruthy();

  await page.addInitScript(sessionId => {
    window.sessionStorage.setItem('zkcode.activeSessionId', sessionId);
    window.localStorage.setItem('zhikun.workbench.default-view', 'development');
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
  await expect(page.locator('[title="已连接"]').first()).toBeVisible();
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
  expect(transcript.messages).toHaveLength(2);
  expect(transcript.messages[0].type).toBe('user');
  expect(transcript.messages[0].content.some(block => block.text?.includes(nonce))).toBe(true);
  expect(transcript.messages[1].type).toBe('assistant');
  expect(transcript.messages[1].content.some(block => block.text === ANSWER)).toBe(true);

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
  await expect(page.locator('[title="已连接"]').first()).toBeVisible();
  await expect(page.getByText(ANSWER).first()).toBeVisible();
  await expect(page.getByText(nonce).first()).toBeVisible();
});
