import { test, expect } from './support/production-test';
import AxeBuilder from '@axe-core/playwright';

// Uses the isolated production backend configured by playwright.production.config.ts.
// Touch is explicit: viewport size alone does not exercise tap behavior.
test.use({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true });

test('real backend mobile: navigation keeps each draft and the current input controls', async ({ page, request }) => {
  const ids: string[] = [];
  for (let index = 0; index < 2; index++) {
    const response = await request.post('/api/sessions', { data: { model: 'qwen3.8-max-0902' } });
    expect(response.status()).toBe(201);
    ids.push((await response.json()).sessionId);
  }
  await page.addInitScript(id => sessionStorage.setItem('zkcode.activeSessionId', id), ids[0]);
  await page.goto('/');
  const bar = page.getByTestId('mobile-prompt-bar');
  const input = page.getByRole('textbox', { name: '输入消息' });
  await expect(bar).toBeVisible();
  await expect(page.getByTestId('mobile-persistent-actions')).toBeVisible();
  await input.fill('mobile draft A');
  await page.getByRole('button', { name: '打开会话列表', exact: true }).tap();
  await page.locator(`[title^="${ids[1]} ·"]`).tap();
  await expect(input).toHaveValue('');
  await input.fill('mobile draft B');
  await page.getByRole('button', { name: '打开会话列表', exact: true }).tap();
  await page.locator(`[title^="${ids[0]} ·"]`).tap();
  await expect(input).toHaveValue('mobile draft A');
  await page.getByRole('button', { name: '命令', exact: true }).tap();
  await expect(input).toHaveValue('/');
  await expect(page.getByTestId('command-palette-footer')).toBeVisible();
  await input.press('Escape');
  await expect(page.getByTestId('command-palette-footer')).toBeHidden();
  await expect(input).toHaveValue('/');
  await expect(page.locator('input[data-mobile-image-input]')).toHaveAttribute('accept', 'image/*');
  for (const label of ['图片附件', '拍照', '文件引用', '发送消息']) {
    const box = await page.getByRole('button', { name: label, exact: true }).boundingBox();
    expect(box?.width).toBeGreaterThanOrEqual(44);
    expect(box?.height).toBeGreaterThanOrEqual(44);
  }
});

test('real backend mobile: input and settings pass WCAG checks with keyboard navigation', async ({ page, request }) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  const configured = await request.put('/api/config', { data: { theme: 'light' } });
  expect(configured.ok()).toBe(true);
  await page.goto('/');
  await expect(page.getByTestId('mobile-prompt-bar')).toBeVisible();
  await page.getByRole('textbox', { name: '输入消息' }).fill('draft remains while navigating');
  const scan = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa']).analyze();
  expect(scan.violations).toEqual([]);
  await page.getByRole('button', { name: '更多', exact: true }).tap();
  await expect(page.getByRole('dialog', { name: '更多操作' })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(page.getByRole('dialog', { name: '更多操作' })).toBeHidden();
  await expect(page.getByRole('textbox', { name: '输入消息' })).toHaveValue('draft remains while navigating');
});
