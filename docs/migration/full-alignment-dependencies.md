# 全量对齐依赖及本机工具记录

依赖仍以 `Cargo.lock`、`frontend/package-lock.json`、Python runtime/build lock 和 `configuration/dev-toolchain.toml` 为版本事实来源。新增库仅承担现有标准协议/格式处理，不替代 Rust 执行生命周期或费用账本。

## 2026-10-08 必要缺陷修复补充

本轮没有新增 Cargo、npm 或 Python 运行依赖。开发安装器要求 Python `>=3.11.4,<3.12`，用于既有安全解包 API；不收窄 Python 服务自身的支持范围。`dev sync` 不再隐式安装 Homebrew，OCR 安装先确认 `brew --prefix` 成功且返回非空绝对路径。

私有 LSP manifest 增加 `rust-src 1.97.1`，来源和归档 SHA-256 固定在 `configuration/lsp-toolchain.json`。解包后完整源码树按相对 POSIX 路径的 UTF-8 字节排序计算身份；Python 安装器与 Rust 校验器使用同一规则。当前验证树 SHA-256 为 `37f0c50967c8fe5dc7ebdc721a3efba591861f913d156c45f34eec1df727ee07`。rust-analyzer 使用显式 `cargo.sysrootSrc`，不修改全局 rustup，也不在查询时下载。

本轮验收使用隔离安装目录与新构建，现有运行服务继续使用原 LSP manifest。原目录 doctor 的 stale/schema 状态与候选环境分开记录；下文旧轮次的 doctor 通过记录只代表对应历史源码。本轮结果以 [必要修复验收](necessary-fixes-2026-10.md) 和配套门禁记录为准。切换正在使用的服务不属于本轮自动操作。

## JSON Schema 校验

`jsonschema = 0.58.6` 关闭默认特性，只进行有界的本地最终答案校验；远程 schema 引用拒绝，不在请求时下载依赖。它经 `referencing → fluent-uri` 引入 `borrow-or-share 0.2.4`。

首次 `cargo deny check` 中 advisories/bans/sources 通过，licenses 因 `borrow-or-share` 的 `MIT-0` 未列入项目清单而失败。已逐字核对该已锁定 crate 随包 LICENSE 与 [SPDX MIT No Attribution](https://spdx.org/licenses/MIT-0.html)，把具体许可证加入 allowlist，继续保留未知许可证拒绝、漏洞拒绝和来源检查；未关闭依赖门禁。复测结果记录在 `full-alignment-gates.json`。

## React 和 Python

Tailwind 4 与配套 PostCSS/主题类名适配使用现有锁文件；保留目标仓库较新依赖，不直接覆盖源仓库依赖版本。官方 npm 审计阶段结果为零漏洞。Python 使用配置的 3.11，不把系统 Xcode Python 3.9 当作支持的运行环境；`pip check` 和 runtime/build lock 校验已通过。

## 私有 LSP 和 Office

五种语言服务器由私有固定工具链安装器管理，官方来源与内容身份验证后才原子替换 manifest，不覆盖全局工具链，不在工具调用时下载。具体版本、真实语义请求与清理证据见 `full-alignment-tools.md`。

Office/PDF/媒体/OCR/CJK 使用本机原生运行器，保存实际工具、字体与浏览器身份；本轮 41 项通过。当前本机 LibreOffice 是 **LibreOfficeDev 26.8 alpha**，记录如实标识，不冒称稳定版。升级任何工具后需要重新记录身份并运行该验收。

最终冻结后已正式运行 `CARGO_INCREMENTAL=0 ./dev sync --offline --build`，使用锁文件同步前端依赖（零已知漏洞）、核验现有 Python/浏览器环境并从当前源码重建 Rust；没有手工改写构建指纹。随后 `./dev doctor --deep --json` 实际 exit 0、37 项检查全部通过：工具链、五类语言服务、文档工具、OCR/字体、真实 Headless Shell、前端构建、Rust 构建身份、npm/pip 锁及依赖树均有效。服务未启动是 doctor 允许的状态，真实服务执行另由 E2E 验证。

思考参数修复后再次通过相同正式流程，日志 `/tmp/zk-alignment-thinking-dev-sync-final.txt`（SHA-256 `8c5ad06e1fd828d1f8c09b68d6e2b5a57f4b9658a217169769569c5648ea3455`）、`/tmp/zk-alignment-thinking-doctor-final.json`（SHA-256 `053663b0e57926d1cb40cbdf89d7fb3d84acce6c0eb5f5320457ad55b4123a90`）记录在 [当前门禁](full-alignment-gates.json)。新源码正式 debug 构建 1 分 11 秒，doctor 37 项全部通过；独立 release 构建和以其运行的 9 项真实页面 E2E 也已通过。此前日志和活动编辑导致构建指纹过期的失败保留为历史，不代表此次最终结果。
