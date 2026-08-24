# 机器契约

本目录保存 zkcode 发布门禁使用的机器可读契约，不是运行时配置。

| 文件 | 约束 |
|---|---|
| `rest-contract.json` | 必须存在的 REST 路由和方法 |
| `ws-contract.json` | 原生 WebSocket 上行与下行消息种类 |
| `tool-contract.json` | 默认工具集合和工具 schema SHA-256 |
| `ddl-consumers.json` | 单库 SQLite 表及其生产消费者 |

从仓库根目录运行：

```bash
./scripts/parity/check-contracts.sh
```

门禁同时检查跨语言版本、`.env.example` 的受支持默认值、MCP 注册表、Playwright
安装步骤和公开文档的本地链接。契约变更必须与生产 Router、协议类型、数据库迁移、
测试和公开说明在同一个变更中更新；不能只修改 JSON 让门禁通过。
