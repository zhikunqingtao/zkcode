import { test, expect } from '@playwright/test';

/**
 * S13 真实链路冒烟：真前端(vite) + 真 zk-server + 真 LLM（Kimi K3）。
 *
 * 不进默认 CI：仅当 ZK_REAL_BACKEND=1 时执行（需要已配置
 * ZK_LLM_API_KEY 的 zk-server 与 vite dev 同时在跑，见 docs/dev-run.md）。
 * 运行：npm run test:e2e:smoke
 *
 * 断言链（对齐 S12 人工验收 a–e）：
 *   WS 建立（"已连接"指示器）→ 发送真实消息 → user 气泡呈现 →
 *   WS 帧观测 stream_delta / message_complete / session_list_updated 且无
 *   error 帧 → 刷新后消息从后端恢复（session_restored + REST messages）。
 */
const REAL_BACKEND = process.env.ZK_REAL_BACKEND === '1';

test.describe('chat smoke (real zk-server + real LLM)', () => {
    test.skip(
        !REAL_BACKEND,
        'real-backend smoke: set ZK_REAL_BACKEND=1 with zk-server + vite running',
    );

    test('round trip: send → stream → complete → restore after reload', async ({ page }) => {
        test.setTimeout(180_000);

        // 收集原生 WS 下行帧的 type 序列（协议层断言依据）。
        const frameTypes: string[] = [];
        page.on('websocket', ws => {
            if (!ws.url().includes('/ws')) return;
            ws.on('framereceived', frame => {
                try {
                    const parsed = JSON.parse(String(frame.payload)) as {
                        type?: string;
                    };
                    if (parsed.type) frameTypes.push(parsed.type);
                } catch {
                    // 非 JSON 帧（心跳等）忽略
                }
            });
        });

        await page.goto('/');
        // a. WS 连接建立（开发工作台 StatusBar 仅以 title 呈现，双兼容匹配）
        const connectedIndicator = page
            .locator('[title="已连接"]')
            .or(page.getByText('已连接'))
            .first();
        await expect(connectedIndicator)
            .toBeVisible({ timeout: 20_000 });

        // 发送带 nonce 的真实消息（nonce 保证断言目标唯一且可在恢复后定位）
        const nonce = `E2E-${Date.now()}`;
        await page.getByRole('textbox', { name: '输入消息' })
            .fill(`请只回复两个字：收到。（校验码 ${nonce}，无需复述）`);
        await page.locator('button[aria-label="发送消息"]').click();

        // b. user 气泡即时呈现（乐观渲染）
        await expect(page.getByText(nonce).first())
            .toBeVisible({ timeout: 15_000 });

        // c. 流式完成：message_complete 帧到达（thinking 模型最长可达分钟级）
        await expect
            .poll(() => frameTypes.includes('message_complete'), {
                timeout: 150_000,
                intervals: [1_000],
            })
            .toBe(true);
        expect(frameTypes).toContain('session_restored');
        expect(frameTypes).toContain('stream_delta');
        expect(frameTypes).toContain('session_list_updated');
        expect(frameTypes.filter(type => type === 'error')).toHaveLength(0);

        // d/e. 刷新后：WS 重绑 + 消息从后端 REST 恢复
        await page.reload();
        await expect(connectedIndicator)
            .toBeVisible({ timeout: 20_000 });
        await expect(page.getByText(nonce).first())
            .toBeVisible({ timeout: 20_000 });
    });
});
