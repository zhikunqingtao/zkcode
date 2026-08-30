//! zk-server 运行配置——环境变量装配（U3 双轨端口 / D6 默认库路径 / D7 鉴权模式）。
//!
//! 环境变量全表（均带开发态默认值，见 `Config::from_env`）：
//!
//! | 变量 | 默认 | 语义 |
//! |---|---|---|
//! | `ZK_PORT` | `8082` | 监听端口（开发轨 8082；验收轨以 `ZK_PORT=8080` 切换，U3） |
//! | `ZK_HOST` | `127.0.0.1` | 监听地址（macOS 本地 Beta 仅接受 loopback IP） |
//! | `ZK_DB_PATH` | `.zk/data.db` | `SQLite` 库路径（D6，相对启动目录） |
//! | `ZK_DEMO_CREDENTIAL_DB` | `configuration/bootstrap/demo-credentials.db` | 公开、只读的首次启动凭据种子；不是用户运行库 |
//! | `ZK_DEV_ALLOW_DEMO_CREDENTIAL` | `0` | 是否允许导入公开 demo 凭据；仅接受 `0` / `1` |
//! | `ZK_DEFAULT_MODEL` | `qwen3.8-max` | 创建会话缺省模型 |
//! | `ZK_AUTH_MODE` | `localhost` | 鉴权模式（`localhost` / `lan_token`，对齐旧 `auth.mode`；只影响 `/api/auth/*` 上报与 token 下发，准入判定恒走 `access_guard`） |
//! | `ZK_STATIC_DIR` | 自动探测 `resources/static` | 静态资源根（`remote.html`；旧 `src/main/resources/static`） |
//! | `ZK_CORS_ALLOWED_ORIGINS` | 空 | 追加 CORS 白名单（逗号分隔，对齐旧 `CORS_ALLOWED_ORIGINS`） |
//! | `ZK_LLM_BASE_URL` | `DashScope` 兼容模式端点 | `OpenAI` 兼容 provider 端点（S9；Moonshot/Kimi 填 `https://api.moonshot.cn/v1`） |
//! | `ZK_LLM_API_KEY` | 空 | provider 密钥（S9；未配置时 chat 请求期回 `query_error`，密钥全路径脱敏不落日志） |
//! | `ZK_LOG` / `RUST_LOG` | `info` | tracing env-filter |
//! | `ZK_WORKSPACE_ALLOWED_ROOTS` | 空 | Projects 域 workspace 白名单根（逗号分隔绝对路径；空 = 不设限但受本地选择器守卫，2.1） |
//! | `ZK_WORKSPACE_DEFAULT_ROOT` | 进程当前目录 | 目录浏览缺省起点（对齐旧 `app.workspace.default-root=user.dir`） |
//! | `ZK_LOCAL_PICKER_ENABLED` | `false` | 本地目录选择/浏览/创建开关（对齐旧 `app.workspace.local-picker-enabled`） |
//! | `ZK_PYTHON_ENABLED` | `true` | Python 侧车总开关（false = 不启动进程、不注册桥接工具，2.6） |
//! | `ZK_PYTHON_UDS` | `~/.zkcode/python.sock` | 侧车 UDS 路径（决策 D-P2-2；旧端为 loopback TCP `127.0.0.1:8000`） |
//! | `ZK_PYTHON_SERVICE_DIR` | `python-service` | uvicorn 工作目录（对齐旧 `PythonProcessManager` :92） |
//! | `ZK_PYTHON_CMD` | 自动探测 | Python 解释器（仅项目 .venv/venv 或 Python 3.11/3.12） |
//! | `ZK_PYTHON_HEALTH_CHECK_INTERVAL_MS` | `30000` | 健康轮询间隔（对齐旧 `python.service.health-check-interval`） |
//! | `ZK_SCRATCHPAD_SYSTEM_ROOT` | `{workspace_default_root}/.zk/scratchpad` | 服务端自有暂存区根（对齐旧 `zhikuncode.scratchpad.system-root`，2.5） |
//! | `ZK_AGENT_ENABLED` | `true` | 已验收的生产子 Agent 总开关 |
//! | `ZK_AGENT_WRITE_ENABLED` | `true` | 子 Agent 写工具开关，仍受统一 Admission 约束 |
//! | `ZK_SWARM_ENABLED` | `true` | 已验收的生产 Coordinator/Swarm 开关 |
//! | `ZK_WORKTREE_ENABLED` | `false` | Worktree 总开关；真实 Git 验收前恒保持关闭 |
//!
//! 非法值（端口非数字等）直接启动失败（fail fast、明确报错），不静默回退。
//!
//! # 特性标志（`ZK_FEATURE_<NAME>` / `FEATURE_<NAME>`）
//!
//! 特性开关不在上表内逐条列举——它们由 [`zk_core::FeatureFlags`] 统一装配（出厂
//! 默认值逐字对齐旧 `application.yml` 的 `features.flags` 节，环境变量覆盖优先，
//! 完整前缀与优先级链见 `zk_core::feature_flags` 模块文档）。`Config` 只把工具注册
//! 期要用的两个开关（`WEB_BROWSER_TOOL` / `GIT_ENHANCED_TOOL`）投影成布尔字段，
//! 取值一律经 flag 表而非独立解析，避免同一开关出现两个事实源。
//!
//! 注意语义差异：flag 的环境变量按旧 `Boolean.parseBoolean` 解析——非 `true`
//! （含空串与拼错的值）一律为 `false`，**不**走上文的 fail-fast 路径。这是对旧实现
//! 的有意对齐（旧 `FeatureFlagService.getFeatureValue` 即如此），故
//! `ZK_FEATURE_WEB_BROWSER_TOOL` 由原先的「非布尔值拒绝启动」改为「非 `true` 即
//! 关闭」；两个开关的 `true` / `false` 正常取值行为不变。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use zk_core::FeatureFlags;
use zk_core::feature_flags;

/// 开发轨默认端口（U3：与旧系统 8080 并行可跑，便于对照调试）。
pub const DEFAULT_PORT: u16 = 8082;

/// macOS 本地 Beta 的默认且受支持监听地址：loopback。
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// 鉴权模式：localhost 免认证（旧 `auth.mode` 缺省值）。
pub const AUTH_MODE_LOCALHOST: &str = "localhost";

/// 鉴权模式：局域网 token（旧 `AuthController` 的 `"lan_token"` 分支）。
pub const AUTH_MODE_LAN_TOKEN: &str = "lan_token";

/// 静态资源目录（相对路径，旧 `src/main/resources/static`）。
pub const STATIC_DIR_REL: &str = "resources/static";

/// Python 侧车 UDS 的默认相对路径（相对用户主目录，决策 D-P2-2）。
pub const DEFAULT_PYTHON_SOCKET_REL: &str = ".zkcode/python.sock";

/// `python-service` 目录默认值（对齐旧 `PythonProcessManager` :92
/// `Path.of("python-service")`，相对进程启动目录）。
pub const DEFAULT_PYTHON_SERVICE_DIR: &str = "python-service";

/// 健康轮询间隔默认值（对齐旧 `python.service.health-check-interval: 30000`）。
pub const DEFAULT_PYTHON_HEALTH_CHECK_INTERVAL_MS: u64 = 30_000;

/// zk-server 运行配置（不可变，启动期一次性装配）。
#[derive(Clone, Debug)]
#[allow(clippy::struct_excessive_bools)] // 配置聚合体：布尔即开关语义，拆枚举反而丢失环境变量一对一映射
pub struct Config {
    /// 监听地址（默认 [`DEFAULT_HOST`]：仅本机可达）。
    pub host: String,
    /// 监听端口（默认 8082 开发轨；验收轨经 `ZK_PORT=8080` 切换）。
    pub port: u16,
    /// `SQLite` 库文件路径（D6 默认 `zkcode/.zk/data.db`，此处以相对路径表达）。
    pub db_path: PathBuf,
    /// 仓库随附的只读公共 demo 凭据种子库。它只用于清洁首次启动，绝不是用户库。
    pub demo_credentials_path: PathBuf,
    /// 是否允许导入公开 demo 凭据。源码开发默认关闭，且环境变量仅接受 0/1。
    pub demo_credential_allowed: bool,
    /// 会话快照目录。测试装配为 `None`，由 `AppState` 分配独占临时目录。
    pub snapshot_dir: Option<PathBuf>,
    /// 创建会话的缺省模型。
    pub default_model: String,
    /// 鉴权模式（[`AUTH_MODE_LOCALHOST`] / [`AUTH_MODE_LAN_TOKEN`]，旧 `auth.mode`）。
    ///
    /// 只决定 `/api/auth/status` 的上报形状与 `/api/auth/token` 是否下发 token；
    /// 请求准入恒由 [`crate::middleware::access_guard`] 判定——同旧
    /// `RemoteAccessSecurityFilter` 全程不读 `auth.mode`。
    pub auth_mode: String,
    /// access token 持久化路径；`None` = 只在进程内存活（测试装配用）。
    pub access_token_path: Option<PathBuf>,
    /// 静态资源根（`remote.html` 等；旧 `src/main/resources/static`）。
    pub static_dir: PathBuf,
    /// 基础 CORS 白名单之外的额外允许来源（逗号分隔展开）。
    pub extra_cors_origins: Vec<String>,
    /// `OpenAI` 兼容 provider 端点（S9 引擎；默认 `DashScope` 兼容模式）。
    pub llm_base_url: String,
    /// provider 密钥（`ApiKey` newtype——`Config` 派生 Debug 亦恒脱敏）。
    pub llm_api_key: zk_llm::ApiKey,
    /// Projects 域 workspace 白名单根（canonical 目录；空 = 不设限，2.1）。
    pub workspace_allowed_roots: Vec<PathBuf>,
    /// 目录浏览缺省起点（旧 `defaultWorkspaceRoot`，默认进程当前目录）。
    pub workspace_default_root: String,
    /// 本地目录选择/浏览/创建开关（旧 `local-picker-enabled`，默认关闭）。
    pub local_picker_enabled: bool,
    /// Python 侧车总开关（2.6；false = 不启动进程且不注册桥接工具）。
    pub python_enabled: bool,
    /// 侧车 UDS 路径（`uvicorn --uds`；决策 D-P2-2）。
    pub python_uds_path: PathBuf,
    /// `python-service` 目录（uvicorn 工作目录）。
    pub python_service_dir: PathBuf,
    /// Python 解释器（`None` = 按 `start.sh` 顺序自动探测）。
    pub python_command: Option<String>,
    /// 侧车健康轮询间隔（旧 `python.service.health-check-interval`）。
    pub python_health_check_interval: Duration,
    /// 子代理总开关。WP-13 真实 Kimi/持久化验收完成后默认开启。
    pub agent_enabled: bool,
    /// 子代理写工具开关。默认开启但仍逐调用经过统一 Admission；显式关闭时
    /// 不会把 Write/Edit/Bash 暴露进子代理工具规格。
    pub agent_write_enabled: bool,
    /// Worktree 能力总开关。真实 Git 验收完成前必须保持关闭。
    pub worktree_enabled: bool,
    /// Swarm 能力总开关。真实 Coordinator/Kimi/重启门禁完成后默认开启。
    pub swarm_enabled: bool,
    /// 特性标志表（旧 `FeatureFlagService` 单例 Bean 的等价物）。
    ///
    /// 启动期装配一次，`Arc` 共享给 `AppState` 与后续各消费方；运行时改写
    /// （旧 `setFeatureValue`）对所有持有者立即可见，故 `Config` 虽不可变，
    /// flag 表本身是可变的共享状态。
    pub feature_flags: Arc<FeatureFlags>,
    /// `WebBrowser` 工具注册门控（旧 feature flag `WEB_BROWSER_TOOL`）。
    ///
    /// 值来自 `feature_flags`（启动期投影），不再独立解析环境变量。
    pub feature_web_browser_tool: bool,
    /// `Git` 增强工具注册门控（旧 feature flag `GIT_ENHANCED_TOOL`）。
    ///
    /// 值来自 `feature_flags`（启动期投影），不再独立解析环境变量。
    pub feature_git_enhanced_tool: bool,
    /// 服务端自有暂存区根（旧 `zhikuncode.scratchpad.system-root`）。
    pub scratchpad_system_root: PathBuf,
    /// MCP 能力注册表文件（旧 `${MCP_REGISTRY_PATH:configuration/mcp/mcp_capability_registry.json}`）。
    ///
    /// 平铺进 `Config` 而非由 `McpCapabilityRegistry::load_default()` 直接读
    /// 环境变量：注册表的增删改会**落盘**，测试装配必须能把它指向 temp，否则
    /// `POST /api/mcp/capabilities` 的集成测试会写进仓库工作树。
    pub mcp_registry_path: PathBuf,
    /// MCP 服务器信任表文件（旧 `~/.zhikun/mcp-trusted.json`）。
    ///
    /// `None` = 不落盘（测试装配），语义同 [`Self::access_token_path`]。
    pub mcp_trust_file: Option<PathBuf>,
    /// Admin 密码的 `SHA-256` 十六进制哈希（Batch 8B；旧 `admin.password` 配置项
    /// 经 `AdminController` 构造期一次性哈希）。
    ///
    /// `None` = 未配置（`ZK_ADMIN_PASSWORD` 缺失 / 空白）——此时
    /// `/api/admin/login` 回 503、`/api/admin/status` 的 `configured=false`。
    pub admin_password_hash: Option<String>,
    /// OSS endpoint（旧 `zhikuncode.oss.endpoint`；本端仅用于剪贴板图片 URL
    /// 信任校验——OSS 发布链路其余能力不在迁移范围）。`None` = 未配置，url
    /// 附件一律拒绝（fail-closed）。
    pub oss_endpoint: Option<String>,
    /// OSS bucket（旧 `zhikuncode.oss.bucket`）。
    pub oss_bucket: Option<String>,
    /// OSS 对象 key 前缀（旧 `zhikuncode.oss.prefix`，缺省 `zhikuncode-artifacts`）。
    pub oss_prefix: String,
}

impl Config {
    /// 从环境变量装配；非法值（端口/监听地址形态）返回错误描述。
    ///
    /// # Errors
    ///
    /// `ZK_PORT` 非法（非数字 / 越界）、`ZK_LOCAL_PICKER_ENABLED` 非布尔、
    /// `ZK_WORKSPACE_ALLOWED_ROOTS` 含不可用目录时返回 `Err`，进程应启动失败。
    pub fn from_env() -> Result<Self, String> {
        let port = match std::env::var("ZK_PORT") {
            Ok(raw) if raw.trim().is_empty() => DEFAULT_PORT,
            Ok(raw) => raw
                .trim()
                .parse::<u16>()
                .map_err(|_| format!("invalid ZK_PORT value: {raw:?} (expected u16)"))?,
            Err(_) => DEFAULT_PORT,
        };
        let host = env_or("ZK_HOST", DEFAULT_HOST);
        // flag 表先于其余字段装配：两个工具门控字段是它的投影，必须同源。
        let feature_flags = Arc::new(FeatureFlags::from_env());
        let host_ip = host
            .parse::<std::net::IpAddr>()
            .map_err(|_| format!("invalid ZK_HOST value: {host:?} (expected IP address)"))?;
        if !host_ip.is_loopback() {
            return Err(format!(
                "unsupported ZK_HOST value: {host:?} (macOS local Beta requires a loopback IP)"
            ));
        }
        Ok(Self {
            host,
            port,
            db_path: PathBuf::from(env_or("ZK_DB_PATH", ".zk/data.db")),
            demo_credentials_path: PathBuf::from(env_or(
                "ZK_DEMO_CREDENTIAL_DB",
                "configuration/bootstrap/demo-credentials.db",
            )),
            demo_credential_allowed: parse_zero_one_env("ZK_DEV_ALLOW_DEMO_CREDENTIAL")?,
            snapshot_dir: Some(PathBuf::from(env_or(
                "ZK_SNAPSHOT_DIR",
                &zk_core::paths::user_config_dir()
                    .join(zk_engine::SNAPSHOT_DIR_NAME)
                    .to_string_lossy(),
            ))),
            default_model: env_or("ZK_DEFAULT_MODEL", "qwen3.8-max"),
            auth_mode: env_or("ZK_AUTH_MODE", AUTH_MODE_LOCALHOST),
            access_token_path: Some(crate::access_token::default_token_path()),
            static_dir: PathBuf::from(env_or("ZK_STATIC_DIR", &default_static_dir())),
            extra_cors_origins: std::env::var("ZK_CORS_ALLOWED_ORIGINS")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            llm_base_url: env_or("ZK_LLM_BASE_URL", zk_llm::DASHSCOPE_BASE_URL),
            llm_api_key: zk_llm::ApiKey::new(env_or("ZK_LLM_API_KEY", "")),
            workspace_allowed_roots: parse_allowed_roots(
                &std::env::var("ZK_WORKSPACE_ALLOWED_ROOTS").unwrap_or_default(),
            )?,
            workspace_default_root: env_or("ZK_WORKSPACE_DEFAULT_ROOT", &default_root()),
            local_picker_enabled: parse_bool_env("ZK_LOCAL_PICKER_ENABLED", false)?,
            python_enabled: parse_bool_env("ZK_PYTHON_ENABLED", true)?,
            python_uds_path: PathBuf::from(env_or("ZK_PYTHON_UDS", &default_python_socket())),
            python_service_dir: PathBuf::from(env_or(
                "ZK_PYTHON_SERVICE_DIR",
                DEFAULT_PYTHON_SERVICE_DIR,
            )),
            python_command: std::env::var("ZK_PYTHON_CMD")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            python_health_check_interval: Duration::from_millis(parse_u64_env(
                "ZK_PYTHON_HEALTH_CHECK_INTERVAL_MS",
                DEFAULT_PYTHON_HEALTH_CHECK_INTERVAL_MS,
            )?),
            agent_enabled: parse_bool_env("ZK_AGENT_ENABLED", true)?,
            agent_write_enabled: parse_bool_env("ZK_AGENT_WRITE_ENABLED", true)?,
            worktree_enabled: parse_bool_env("ZK_WORKTREE_ENABLED", false)?,
            swarm_enabled: parse_bool_env("ZK_SWARM_ENABLED", true)?,
            feature_web_browser_tool: feature_flags.is_enabled(feature_flags::WEB_BROWSER_TOOL),
            feature_git_enhanced_tool: feature_flags.is_enabled(feature_flags::GIT_ENHANCED_TOOL),
            feature_flags,
            scratchpad_system_root: PathBuf::from(env_or(
                "ZK_SCRATCHPAD_SYSTEM_ROOT",
                &default_scratchpad_root(),
            )),
            mcp_registry_path: PathBuf::from(env_or(
                zk_mcp::capability_registry::ENV_VAR_REGISTRY_PATH,
                zk_mcp::capability_registry::DEFAULT_REGISTRY_PATH,
            )),
            mcp_trust_file: Some(
                zk_core::paths::user_config_dir().join(crate::mcp::TRUST_FILE_NAME),
            ),
            // 旧 `AdminController` 构造期对 `admin.password` 做 SHA-256；此处只在
            // 非空白时哈希，空白 / 缺失 → `None`（对齐 `isBlank()` 分支）。原始值
            // 不 trim 后再哈希——与旧 `hashPassword(adminPassword)` 逐字对齐。
            admin_password_hash: std::env::var("ZK_ADMIN_PASSWORD")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(|value| crate::api::admin::sha256_hex(&value)),
            oss_endpoint: std::env::var("ZK_OSS_ENDPOINT")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            oss_bucket: std::env::var("ZK_OSS_BUCKET")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            oss_prefix: env_or("ZK_OSS_PREFIX", "zhikuncode-artifacts"),
        })
    }

    /// 是否为局域网 token 鉴权模式（旧 `"lan_token".equals(authMode)`）。
    #[must_use]
    pub fn is_lan_token_mode(&self) -> bool {
        self.auth_mode == AUTH_MODE_LAN_TOKEN
    }

    /// 测试用最小配置（内存库 / 固定默认模型 / localhost 鉴权）。
    ///
    /// `#[doc(hidden)]`：仅供集成测试装配 `AppState`，生产代码禁止使用。
    #[doc(hidden)]
    #[must_use]
    pub fn test_config() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: DEFAULT_PORT,
            db_path: PathBuf::from(":memory:"),
            demo_credentials_path: Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../configuration/bootstrap/demo-credentials.db"),
            demo_credential_allowed: false,
            snapshot_dir: None,
            default_model: "qwen3.8-max".into(),
            auth_mode: AUTH_MODE_LOCALHOST.into(),
            // 测试装配不落盘：token 只存活于进程内，绝不污染用户 `~/.zk/`。
            access_token_path: None,
            static_dir: PathBuf::from(default_static_dir()),
            extra_cors_origins: Vec::new(),
            llm_base_url: zk_llm::DASHSCOPE_BASE_URL.to_owned(),
            llm_api_key: zk_llm::ApiKey::new(""),
            workspace_allowed_roots: Vec::new(),
            workspace_default_root: default_root(),
            local_picker_enabled: false,
            // 单测 / 集成测试不起真实 Python 进程：侧车关闭 → 桥接工具不注册，
            // 既保住 2.3 的 9 件工具断言，也避免测试期误触外部进程。
            python_enabled: false,
            python_uds_path: PathBuf::from("/tmp/zkcode-test-python.sock"),
            python_service_dir: PathBuf::from(DEFAULT_PYTHON_SERVICE_DIR),
            python_command: None,
            python_health_check_interval: Duration::from_millis(
                DEFAULT_PYTHON_HEALTH_CHECK_INTERVAL_MS,
            ),
            agent_enabled: false,
            agent_write_enabled: false,
            worktree_enabled: false,
            swarm_enabled: false,
            // 测试装配不读环境变量：flag 取出厂默认（两个门控的出厂值皆为 `true`，
            // 与下方投影字段一致），保证测试结果不受宿主环境影响。
            feature_flags: Arc::new(FeatureFlags::with_defaults()),
            feature_web_browser_tool: true,
            feature_git_enhanced_tool: true,
            scratchpad_system_root: PathBuf::from(default_scratchpad_root()),
            // 测试装配指向不存在的 temp 路径：注册表起手为空表（`load` 只
            // warn），而增删改的落盘落在 temp，绝不污染仓库工作树。
            mcp_registry_path: std::env::temp_dir()
                .join("zkcode-test-mcp")
                .join("mcp_capability_registry.json"),
            // 同 `access_token_path`：测试不写用户 `~/.zk/`。
            mcp_trust_file: None,
            // 测试装配不配置 admin 密码：`/api/admin/login` 恒 503，不依赖宿主环境。
            admin_password_hash: None,
            // 测试装配不配置 OSS：url 附件信任校验恒拒绝（fail-closed）。
            oss_endpoint: None,
            oss_bucket: None,
            oss_prefix: "zhikuncode-artifacts".into(),
        }
    }
}

/// 侧车 UDS 默认路径：`$HOME/.zkcode/python.sock`（`HOME` 缺失时回落
/// `/tmp/.zkcode/python.sock`，保证路径长度远低于 `sun_path` 上限）。
fn default_python_socket() -> String {
    let home = std::env::var("HOME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "/tmp".to_owned());
    PathBuf::from(home)
        .join(DEFAULT_PYTHON_SOCKET_REL)
        .to_string_lossy()
        .into_owned()
}

/// 服务端自有暂存区根默认值（旧 `@Value` 默认表达式
/// `${app.working-dir:${user.dir}}` + 旧暂存区目录名；#65 起目录名统一为
/// `.zk/scratchpad`，经 `zk_core::paths::scratchpad_dir` 解析）。
fn default_scratchpad_root() -> String {
    zk_core::paths::scratchpad_dir(Path::new(&default_root()))
        .to_string_lossy()
        .into_owned()
}

/// 静态资源根探测：优先进程启动目录下的 `resources/static`（部署形态），其次
/// `crates/zk-server/resources/static`（`cargo run` 于仓库根），均不存在时回落
/// 编译期 crate 根下的绝对路径（开发态兜底）。旧端由 jar 内 classpath 资源承担，
/// 无对应配置项。
fn default_static_dir() -> String {
    let candidates = [
        PathBuf::from(STATIC_DIR_REL),
        PathBuf::from("crates/zk-server").join(STATIC_DIR_REL),
    ];
    for candidate in candidates {
        if candidate.is_dir() {
            return candidate.to_string_lossy().into_owned();
        }
    }
    concat!(env!("CARGO_MANIFEST_DIR"), "/resources/static").to_owned()
}

/// 目录浏览缺省起点兜底：进程当前目录（旧 `user.dir` 等价）。
fn default_root() -> String {
    std::env::current_dir()
        .map_or_else(|_| "/".to_owned(), |dir| dir.to_string_lossy().into_owned())
}

/// 解析 allowed roots（逗号分隔）：逐个 canonicalize + 目录校验，任一不可用
/// 即启动失败（对齐旧 `configuredAllowedRoots` 的 fail-fast 语义），去重保序。
fn parse_allowed_roots(raw: &str) -> Result<Vec<PathBuf>, String> {
    let mut roots: Vec<PathBuf> = Vec::new();
    for value in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let canonical = std::fs::canonicalize(value)
            .ok()
            .filter(|path| path.is_dir())
            .ok_or_else(|| format!("configured workspace allowed root is unavailable: {value}"))?;
        if !roots.contains(&canonical) {
            roots.push(canonical);
        }
    }
    Ok(roots)
}

/// 解析布尔环境变量：缺省/空串取默认，其余仅接受 `true`/`false`（不区分大小写）。
fn parse_bool_env(key: &str, default: bool) -> Result<bool, String> {
    match std::env::var(key) {
        Ok(raw) if raw.trim().is_empty() => Ok(default),
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(format!(
                "invalid {key} value: {raw:?} (expected true/false)"
            )),
        },
        Err(_) => Ok(default),
    }
}

/// Parse a security-sensitive opt-in flag. Absence means disabled; every
/// explicit value other than the canonical `0` or `1` is rejected.
fn parse_zero_one_env(key: &str) -> Result<bool, String> {
    match std::env::var(key) {
        Err(std::env::VarError::NotPresent) => Ok(false),
        Ok(raw) => parse_zero_one(key, &raw),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(format!("invalid {key} value (expected 0 or 1)"))
        }
    }
}

fn parse_zero_one(key: &str, raw: &str) -> Result<bool, String> {
    match raw {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(format!("invalid {key} value (expected 0 or 1)")),
    }
}

/// 解析 u64 环境变量：缺省/空串取默认，非法值 fail-fast。
fn parse_u64_env(key: &str, default: u64) -> Result<u64, String> {
    match std::env::var(key) {
        Ok(raw) if raw.trim().is_empty() => Ok(default),
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("invalid {key} value: {raw:?} (expected non-negative integer)")),
        Err(_) => Ok(default),
    }
}

/// 读环境变量，缺省或空串时取默认值。
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

#[cfg(test)]
mod tests {
    use super::parse_zero_one;

    #[test]
    fn demo_credential_opt_in_accepts_only_canonical_zero_or_one() {
        assert_eq!(parse_zero_one("FLAG", "0"), Ok(false));
        assert_eq!(parse_zero_one("FLAG", "1"), Ok(true));
        for invalid in ["", "true", "false", " 1", "1 ", "01", "2"] {
            assert!(
                parse_zero_one("FLAG", invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }
}
