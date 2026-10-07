import { expect, test } from '@playwright/test';

test.describe('Developer workbench', () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      // Obsolete preferences must not restore the removed UI or erase another preference.
      localStorage.setItem('zhikun.workbench.enabled', 'true');
      localStorage.setItem('zhikun.workbench.default-view', 'simple');
      localStorage.setItem('zhikun.turn-view.v1', JSON.stringify({ state: { density: 'detailed', expandOverrides: {} }, version: 2 }));
    });
    await page.route('**/api/**', route => {
      const path = new URL(route.request().url()).pathname;
      if (!path.startsWith('/api/')) return route.continue();
      return route.fulfill({ json: path === '/api/sessions'
        ? { sessions: [], hasMore: false }
        : path === '/api/skills' ? [] : {} });
    });
    await page.routeWebSocket(/\/ws(?:\?|$)/, () => {
      // Keep an isolated idle connection; closing it would intentionally trigger reconnect toasts.
    });
  });

  for (const width of [1440, 390]) {
    test(`ignores a saved simple preference without losing a draft at ${width}px`, async ({ page }) => {
      await page.setViewportSize({ width, height: 900 });
      await page.goto('/', { waitUntil: 'domcontentloaded' });
      await expect(page.getByRole('heading', { name: '今天想构建什么？' })).toBeVisible();
      await expect(page.getByRole('tablist', { name: '工作台视图' })).toHaveCount(0);
      await expect(page.getByRole('tab', { name: '简洁工作台' })).toHaveCount(0);
      const input = page.getByRole('textbox', { name: '输入消息' });
      await input.fill('保留这段未发送内容');
      if (width === 1440) {
        const density = page.getByRole('tablist', { name: '显示方式' });
        await expect(density.getByRole('tab', { name: '完整过程', exact: true })).toHaveAttribute('aria-selected', 'true');
        await density.getByRole('tab', { name: '标准', exact: true }).click();
      } else {
        await page.getByRole('button', { name: '显示方式：完整过程，点击切换', exact: true }).click();
        await page.getByRole('button', { name: /^标准/ }).click();
      }
      await expect(input).toHaveValue('保留这段未发送内容');
      await expect.poll(() => page.evaluate(() => JSON.parse(localStorage.getItem('zhikun.turn-view.v1')!).state.density)).toBe('balanced');
      expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(width);
    });
  }
});
