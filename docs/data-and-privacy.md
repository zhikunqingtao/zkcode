# 数据与隐私

zkcode 是本地运行的软件，但“本地运行”不等于“所有数据只留在本机”。模型请求、
联网工具和第三方扩展会按你的配置把必要数据发送给外部服务。使用前应了解下面的
存储位置和数据流。

## 本地存储

| 位置 | 可能包含的内容 | 处理建议 |
|---|---|---|
| `configuration/bootstrap/demo-credentials.db` | 所有下载者共享、可提取的限额演示凭据 | 只用于首次体验；不放入任何私人凭据，不用于敏感内容 |
| 仓库根 `.env` | 模型 API key 和本地配置 | 不提交、不分享；轮换泄露的密钥 |
| 仓库根 `.zk/data.db` | API key、Project、Session、消息、Run、Task、证据、产物元数据和审计记录 | `0600` 私有运行库；视为密钥、对话与项目数据 |
| 仓库根 `.runtime/` | PID、后端/前端日志 | 报障前检查并脱敏 |
| 工作区 `.zk/` | 项目提示、规则、Hook、scratchpad、浏览器回放、Todo 与本地观测事件 | 按项目数据处理；需要时自行决定是否提交其中的规则文件 |
| `~/.zk/` | 本地访问令牌、快照、上传、产物、记忆和 MCP 信任记录 | 视为跨项目私密数据 |
| 仓库根 `.runtime/python.sock` | 运行中的 Python UDS | 临时 IPC 文件，不应复制或共享 |

运行时数据库及 SQLite WAL/SHM、访问令牌文件和 sidecar socket 会限制为当前
macOS 用户访问。它们不能阻止同一用户权限下的恶意进程读取数据，因此 zkcode
不是操作系统级沙箱。公开引导库刻意不受这一保密承诺约束。

## 可能离开本机的数据

- 发送给模型 provider 的内容可能包括系统提示、你的消息、选中的代码与文件内容、
  附件、历史对话、工具结果和错误信息。具体保留策略由所配置 provider 决定。
- WebFetch、WebSearch 和浏览器工具会连接目标网站；目标网站能看到常规网络请求
  元数据和被提交的内容。
- MCP server、Skill、Hook 和项目脚本是独立的执行主体。它们能处理或转发的数据
  取决于其实现和你批准的权限，应在启用前自行审查。
- 使用自定义 `BASE_URL` 时，请把该网关视为模型 provider；它能看到经过它的请求。

zkcode 0.1.x 没有内置向项目维护者发送的产品遥测。可观测性记录写入本地数据库、
工作区 `.zk/` 或 `.runtime/` 日志。依赖下载、模型 provider、目标网站和第三方
扩展各自可能有独立日志或遥测策略。

## 备份

先停止服务，再备份仓库根 `.zk/`、需要保留的工作区 `.zk/` 和用户目录
`~/.zk/`。`.env` 含真实密钥，不建议放入普通云盘或未加密备份；更安全的做法是
从密钥管理器重新配置。

```bash
./dev stop
cp -R .zk "../zkcode-data-backup-$(date +%Y%m%d)"
cp -R "$HOME/.zk" "$HOME/zkcode-user-backup-$(date +%Y%m%d)"
```

恢复时保持 zkcode 停止，把备份放回原位置后再启动。恢复不同版本的数据前先查看
[更新日志](../CHANGELOG.md)。

## 清理与重置

清理会话数据前先停止服务并保留可恢复副本：

```bash
./dev stop
mv .zk "../zkcode-data-before-reset-$(date +%Y%m%d-%H%M%S)"
mv "$HOME/.zk" "$HOME/zkcode-user-before-reset-$(date +%Y%m%d-%H%M%S)"
```

再次启动时会创建新的本地状态。`.env` 和工作区内的 `.zk/` 不会被上述命令移动；
如果目标是完全撤销访问，还需要分别清理工作区状态并在 provider 控制台轮换 API
key。确认备份不再需要后，由你自行安全删除。

发现疑似密钥或隐私泄露时，先停止服务、撤销相关 provider/MCP 凭据，再按
[安全策略](../SECURITY.md) 报告软件漏洞。
