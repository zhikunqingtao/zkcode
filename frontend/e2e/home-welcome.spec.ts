import { expect, test, type Page } from '@playwright/test';

const SESSION_ID = 'newest-session';
const RESTORED_TEXT = '已恢复的开发会话内容';

async function selectExistingSession(page: Page) {
  if ((page.viewportSize()?.width ?? 1440) < 1024) {
    await page.getByRole('button', { name: '打开会话列表', exact: true }).click();
  }
  await page.getByText('最新会话', { exact: true }).click();
  await expect(page.locator('[data-message-uuid="restored-user"]').getByText(RESTORED_TEXT, { exact: true })).toBeVisible();
  await expect.poll(() => page.evaluate(() => sessionStorage.getItem('zkcode.activeSessionId'))).toBe(SESSION_ID);
}

const examples = [
  ['生成一个王者荣耀', '你可以开发一个类似王者荣耀的游戏么？我希望页面越逼真越好，帮我开发一个这样的游戏，跟王者荣耀越像越好，最终需要正常能玩没有明显功能 bug，你在开始动手前所有不确定的问题都要先跟我确认，给我我多个独立方案让我选择。但是在你开始写代码以后，就不要再问我了，都要自动选择能正常落地而且能尽量逼真效果的方案，不要选择注册网站单独购买等方案'],
  ['帮我做一个12306网站动画', "帮我做一个动态html，可视化展示'你后补成功的那一刻，12306后台发送了什么'，要覆盖候补成功的12306后台完整流程，要展示各种数学原理和12306对应系统的架构，最关键的是一定要动态可视化，页面自己动，有很多惊艳漂亮的可视化动画"],
  ['我想投资黄金', '我想做黄金投资，但是不太懂怎么具体操作，也不知道该怎么监控行情，你能帮我每日可以随时跟踪国际国内市场的金价以及银行的积存金的价格。同时监控国家对黄金回购的频率及整体金额趋势。你不是很确定的要先跟我沟通确认'],
  ['帮我做一个宇树科技Excel', '请以2026年8月30日为资料截止时间，优先使用上海证券交易所披露的招股说明书、上市公告和宇树科技官方公开资料，对宇树科技的经营与财务表现进行分析，并生成一份可编辑的Excel分析底稿。请至少保留资料来源、原始数据、核心计算、趋势图表和分析结论；统一单位与报告期口径，不得把预测数据写成已经发生的事实。除文件外，请简要说明你的分析步骤和仍需人工核验的内容。'],
];

test.beforeEach(async ({ page }) => {
  // No live backend or model calls: startup reads only, with a newer session in the list.
  await page.route('**/api/**', route => {
    const path = new URL(route.request().url()).pathname;
    if (!path.startsWith('/api/')) return route.continue();
    const session = { id: SESSION_ID, title: '最新会话', model: 'test', workingDirectory: '/workspace', messageCount: 1, purpose: 'chat' };
    const body = path === '/api/sessions'
      ? { sessions: [session], hasMore: false }
      : path === `/api/sessions/${SESSION_ID}` ? session
      : path === '/api/skills' || path === '/api/interactions/pending' ? [] : {};
    return route.fulfill({ json: body });
  });
  await page.routeWebSocket(/\/ws(?:\?|$)/, socket => {
    let sequence = 0;
    socket.onMessage(raw => {
      const frame = JSON.parse(String(raw)) as {
        type: string; sessionId?: string; bindRequestId?: string; bindingEpoch?: number;
      };
      if (frame.type !== 'bind_session') return;
      expect(frame.sessionId).toBe(SESSION_ID);
      expect(frame.bindRequestId).toBeTruthy();
      expect(frame.bindingEpoch).toBeGreaterThan(0);
      socket.send(JSON.stringify({
        type: 'session_restored', ts: Date.now(), protocolVersion: 4,
        bindRequestId: frame.bindRequestId, bindingEpoch: frame.bindingEpoch,
        eventContext: { protocolVersion: 4, eventId: `home-restore-${++sequence}`,
          sessionId: SESSION_ID, taskId: null, runId: null, sourceTaskId: null, sourceRunId: null, toolUseId: null },
        messages: [{ uuid: 'restored-user', type: 'user', timestamp: 1,
          content: [{ type: 'text', text: RESTORED_TEXT }] }],
        activities: [], totalActivityCount: 0, hasMore: false, snapshotEventSeq: 0,
        metadata: { sessionId: SESSION_ID, model: 'test', permissionMode: 'DEFAULT', status: 'idle', purpose: 'chat' },
      }));
    });
  });
  page.on('dialog', dialog => dialog.accept());
});

for (const viewport of [{ width: 1440, height: 900 }, { width: 390, height: 844 }]) {
  test(`welcome and editable examples at ${viewport.width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize(viewport);
    const writes: string[] = [];
    page.on('request', request => {
      if (request.url().includes('/api/') && request.method() !== 'GET') writes.push(request.url());
    });
    await page.goto('/');
    const heading = page.getByRole('heading', { name: '今天想构建什么？' });
    await expect(heading).toBeVisible();
    const hero = heading.locator('..');
    await expect(hero.getByText('zkcode', { exact: true })).toBeVisible();
    await expect(hero.locator('img')).toBeVisible();
    await expect(hero.getByText('下达指令，剩下的一切交给zkcode')).toBeVisible();
    await expect(hero.getByText(/APOS/)).toHaveCount(0);
    await page.evaluate(() => window.dispatchEvent(new Event('session-list-updated')));
    expect(await page.evaluate(() => sessionStorage.getItem('zkcode.activeSessionId'))).toBeNull();
    await expect(heading).toBeVisible();
    const input = page.getByRole('textbox', { name: '输入消息' });
    for (const [label, prompt] of examples) {
      const button = hero.getByRole('button', { name: label, exact: true });
      await expect(button).toBeInViewport();
      await button.click();
      await expect(input).toHaveValue(prompt);
      await expect(input).toBeFocused();
      await input.fill('可以自行修改');
      await expect(input).toHaveValue('可以自行修改');
      await input.fill('');
    }
    expect(writes).toEqual([]);
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(viewport.width);
    await page.screenshot({ path: testInfo.outputPath(`welcome-${viewport.width}.png`) });
  });
}

test('a committed selection survives reload; clearing it returns home', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByRole('heading', { name: '今天想构建什么？' })).toBeVisible();
  // The existing list activates a candidate; the matching V4 restore commits it.
  await selectExistingSession(page);
  await page.reload();
  await expect(page.locator('[data-message-uuid="restored-user"]').getByText(RESTORED_TEXT, { exact: true })).toBeVisible();
  expect(await page.evaluate(() => sessionStorage.getItem('zkcode.activeSessionId'))).toBe(SESSION_ID);
  await page.getByRole('button', { name: '返回首页', exact: true }).click();
  await expect(page.getByRole('heading', { name: '今天想构建什么？' })).toBeVisible();
  expect(await page.evaluate(() => sessionStorage.getItem('zkcode.activeSessionId'))).toBeNull();
});

for (const width of [390, 900]) {
  test(`menu opens the session list directly at ${width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 844 });
    await page.goto('/');
    await page.getByRole('button', { name: '打开会话列表' }).click();
    await expect(page.getByRole('main').getByRole('button', { name: '新建会话', exact: true })).toBeVisible();
    await expect(page.getByRole('textbox', { name: '搜索会话', exact: true })).toBeVisible();
    await expect(page.getByRole('dialog', { name: '侧边栏' })).toHaveCount(0);
    await expect(page.locator('aside')).toHaveCount(0);
    await page.screenshot({ path: testInfo.outputPath(`session-menu-${width}.png`) });
    await page.getByRole('button', { name: '返回', exact: true }).click();
    await expect(page.getByRole('heading', { name: '今天想构建什么？' })).toBeVisible();
  });
}

for (const width of [390, 900, 1440]) {
  test(`logo clears the selection and returns home at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto('/');
    await expect(page.getByRole('heading', { name: '今天想构建什么？' })).toBeVisible();
    await selectExistingSession(page);
    if (width < 1024) await page.getByRole('button', { name: '打开会话列表' }).click();
    const writes: string[] = [];
    page.on('request', request => {
      if (request.url().includes('/api/') && request.method() !== 'GET') writes.push(request.url());
    });
    await page.getByRole('button', { name: '返回首页' }).click();
    await expect(page.getByRole('heading', { name: '今天想构建什么？' })).toBeVisible();
    expect(await page.evaluate(() => sessionStorage.getItem('zkcode.activeSessionId'))).toBeNull();
    await page.reload();
    await expect(page.getByRole('heading', { name: '今天想构建什么？' })).toBeVisible();
    expect(writes).toEqual([]);
  });
}
