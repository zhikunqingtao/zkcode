# Phase 0 基线（旧系统实采）

> `samples/GET_api-models.json` 在保留原采样元数据的同时，已按 2026-08-31
> 模型迁移更新为当前声明式契约；其余文件仍是原始实采证据。

## 采样信息

- **采样时间**：2026-08-15 19:05–19:08（UTC+8）
- **服务版本**：1.0.0（`/api/health` → `service=ai-code-assistant-backend, java=21.0.10`）
- **启动方式**：官方脚本 `zhikuncode/start.sh`（采毕已 `stop.sh` 停止，8080 不可达）
- **就绪耗时**：15s（HTTP 200 / status=UP）

## 采得文件

| 文件 | 内容 |
|------|------|
| `openapi-baseline.json` | ⚠️ **HTTP 500 错误响应证据**（见下方已知限制） |
| `endpoints-from-source.json` | 源码静态扫描端点清单，**97 端点**（OpenAPI 500 的补偿基线） |
| `samples/*.json` | **19 个端点实采样例**（5 个 Controller 全端点，含 `_meta.status_code`） |

## 端点清单（实采 19 = 5 Controller 全端点：health 4 + auth 2 + models 1 + config 4 + sessions 8）

| 端点 | 方法 | 状态码 | 样例文件 |
|------|------|--------|----------|
| /api/health | GET | 200 | GET_api-health.json |
| /api/health/live | GET | 200 | GET_api-health-live.json |
| /api/health/ready | GET | 200 | GET_api-health-ready.json |
| /api/doctor | GET | 200 | GET_api-doctor.json |
| /api/auth/status | GET | 200 | GET_api-auth-status.json |
| /api/auth/token | GET | 404 | GET_api-auth-token.json（localhost 模式预期） |
| /api/models | GET | 200 | GET_api-models.json（迁移后契约：16 模型；原采样 default=qwen3.7-max） |
| /api/config | GET | 200 | GET_api-config.json |
| /api/config | PUT | 200 | PUT_api-config.json（原值幂等写回） |
| /api/config/project | GET | 200 | GET_api-config-project.json |
| /api/config/project | PUT | 200 | PUT_api-config-project.json（原值幂等写回） |
| /api/sessions | POST | 201 | POST_api-sessions.json |
| /api/sessions | GET | 200 | GET_api-sessions.json |
| /api/sessions/{id} | GET | 200 | GET_api-sessions-id.json |
| /api/sessions/{id} | DELETE | 200 | DELETE_api-sessions-id.json |
| /api/sessions/{id}/resume | POST | 200 | POST_api-sessions-id-resume.json |
| /api/sessions/{id}/compact | POST | 200 | POST_api-sessions-id-compact.json |
| /api/sessions/{id}/export | POST | 200 | POST_api-sessions-id-export-json.json |
| /api/sessions/{id}/messages | GET | 200 | GET_api-sessions-id-messages.json |

## paths 总数

- `/v3/api-docs` 实际返回 **HTTP 500**，无法得到 OpenAPI paths 数（判据 ≥100 未达成，原因见下）
- 补偿口径：源码静态扫描得 **97 个端点**（`endpoints-from-source.json`，覆盖全部 controller）

## 已知限制

1. **OpenAPI 导出失败（旧系统自身 bug）**：`GET /v3/api-docs` → 500，根因
   `NoSuchMethodError: ControllerAdviceBean.<init>(Object)`（springdoc-openapi 2.6.0
   与 Spring Framework 6.2.15 不兼容，栈在 `GenericResponseService.getGenericMapResponse`）。
   修复需改旧仓库 pom（升级 springdoc ≥2.7），违反只读约束，故保留 500 响应为证据。
2. **compact 采样为空会话**：`{success:true, tokensBefore:0, tokensAfter:0}`。
   含真实历史的压缩路径（LLM 摘要触发条件）未采样——需要真实对话上下文，超出只读采样边界。
3. **resume 采样为空会话**：`messages: []`。有历史消息的 resume 形状可从
   `GET /api/sessions/{id}/messages` 样例推导（同为 Message 结构列表）。
4. **export 仅采 format=json**：markdown 格式未采（同型变体，Content-Type 不同）。
5. **POST /api/sessions 请求体**：空对象 `{}`。字段级校验行为：`workingDirectory`
   传值会被 400 拒绝（SESSION_WORKING_DIRECTORY_UNSUPPORTED，源码 SessionController:84-89）。
6. **鉴权样例仅在 localhost 模式**：lan_token/jwt 模式的 200 形状未采（需改配置重启，越界）。

## 脱敏说明

保存前对全部样例执行 `apiKey|token|secret|password`（不区分大小写）与 `sk-` 长串扫描：
命中均为无害字段名（`maxOutputTokens`/`tokensBefore`/`tokensAfter`/404 消息 "Token auth
not enabled"），无任何真实密钥。会话列表中的自由文本 `goalPreview` 已替换为中性示例，
所有工作目录均使用 `/Users/example/...`，不保留采样用户的身份、路径或任务内容。
