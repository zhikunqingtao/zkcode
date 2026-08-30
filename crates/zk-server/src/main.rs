//! zk-server 进程入口——配置装配、可观测初始化、DB 打开与服务生命周期。
//!
//! 启动序列：`Config::from_env`（fail fast）→ tracing（JSON 结构化日志，
//! `ZK_LOG` / `RUST_LOG` / 默认 `info`）→ 旧用户目录迁移
//!（`zk_core::migrate`，失败只告警不阻塞）→ metrics recorder → 打开 `SQLite`
//!（自动建父目录 + PRAGMA + 迁移）→ 装配多提供商 → 装配 Python 侧车（后台
//! 启动，不阻塞监听）→ 绑定监听（`into_make_service_with_connect_info` 为
//! 准入守卫注入对端地址）→ 装配 MCP 客户端管理器（后台连接 + 30s 健康巡检）
//! → 绑定 loopback 监听 → 优雅关停（SIGTERM/Ctrl-C，含侧车与 MCP 优雅停止）。

use std::net::SocketAddr;
use std::sync::Arc;

use zk_db::Db;

use zk_server::config::Config;
use zk_server::metrics_recorder;
use zk_server::python::{PythonSidecar, SidecarConfig};
use zk_server::routes::build_router;
use zk_server::skill::loader as skill_loader;
use zk_server::state::AppState;

#[tokio::main]
#[allow(clippy::too_many_lines)] // process composition and ordered shutdown lifecycle
async fn main() {
    // Process metadata must be available without configuration, filesystem
    // migration, SQLite, or network initialization. This is also what package
    // managers and macOS doctor scripts expect from a conventional CLI.
    let args: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
    if args.len() == 1 && matches!(args[0].to_str(), Some("--version" | "-V")) {
        zk_server::print_version();
        return;
    }
    if args.len() == 1 && matches!(args[0].to_str(), Some("--help" | "-h")) {
        println!(
            "zk-server {}\n\nUsage: zk-server [OPTIONS]\n\nOptions:\n  -V, --version  Print version\n  -h, --help     Print help",
            env!("CARGO_PKG_VERSION")
        );
        return;
    }
    let config = Config::from_env().unwrap_or_else(|err| {
        eprintln!("zk-server: invalid configuration: {err}");
        std::process::exit(1);
    });
    init_tracing();
    // #65：旧用户目录 → `~/.zk/` 的一次性迁移，必须先于任何读取 `~/.zk/` 的
    // 初始化。刻意放在 `init_tracing` **之后**：迁移自身零配置依赖，而其降级
    // 路径只发 warn 日志——没有 subscriber 时这条告警会被丢弃，等于静默失败。
    zk_core::migrate::run_if_needed();
    tracing::info!(
        host = %config.host,
        port = config.port,
        db_path = %config.db_path.display(),
        auth_mode = %config.auth_mode,
        "zk-server starting"
    );
    metrics_recorder::install_once();

    if let Some(parent) = config.db_path.parent()
        && !parent.as_os_str().is_empty()
        && let Err(err) = std::fs::create_dir_all(parent)
    {
        eprintln!(
            "zk-server: cannot create db directory {}: {err}",
            parent.display()
        );
        std::process::exit(1);
    }
    let db = match Db::open(&config.db_path) {
        Ok(db) => db,
        Err(err) => {
            eprintln!("zk-server: cannot open database: {err}");
            std::process::exit(1);
        }
    };

    // 2.7：多提供商装配——优先扫描 `LLM_PROVIDER_*` 环境变量构建注册表；
    // 未配任何 provider key 时退化为 Phase 1 单 provider（`ZK_LLM_BASE_URL` /
    // `ZK_LLM_API_KEY`），行为与 S9 一致。默认模型统一取 `ZK_DEFAULT_MODEL`
    // （与创建会话共用同一配置源）。密钥不落日志（ApiKey 脱敏）。
    let providers = build_provider_registry(&config);
    tracing::info!(
        provider_count = providers.len(),
        model_count = providers.models().len(),
        multi_provider = zk_llm::has_provider_env(),
        default_model = %config.default_model,
        "wiring chat engine"
    );
    let mut state = AppState::new(db, config.clone()).with_providers(providers);
    // Task 4 Step 6：启动时合并 DB 保存的 LLM 密钥——若 DB 有密钥则重建
    // registry 并热替换；无 DB 密钥时保留环境变量 / Phase 1 初始 registry。
    if let Err(error) = state.merge_db_llm_keys().await {
        tracing::error!(%error, "LLM credential startup migration failed");
        eprintln!("zk-server: LLM credential startup migration failed");
        std::process::exit(1);
    }
    match state.db.interrupt_active_tasks().await {
        Ok(count) if count > 0 => {
            tracing::warn!(count, "marked tasks interrupted by process restart");
        }
        Ok(_) => {}
        Err(error) => {
            tracing::error!(%error, "failed to reconcile durable tasks after restart");
        }
    }
    match state.db.interrupt_active_swarms().await {
        Ok(count) if count > 0 => {
            tracing::warn!(count, "marked Swarms interrupted by process restart");
        }
        Ok(_) => {}
        Err(error) => {
            tracing::error!(%error, "failed to reconcile durable Swarms after restart");
        }
    }

    // 2.6：Python 侧车装配。进程启动与健康巡检全部在后台任务中进行——HTTP
    // 监听绑定不等待 uvicorn 就绪（保住「启动 <5s」判据），侧车未就绪期间
    // 桥接工具经能力域门控优雅降级，核心对话不受影响。
    let sidecar = config.python_enabled.then(|| {
        Arc::new(PythonSidecar::new(
            SidecarConfig {
                socket: config.python_uds_path.clone(),
                service_dir: config.python_service_dir.clone(),
                workspace_root: config.workspace_default_root.clone().into(),
                python_command: config.python_command.clone(),
                health_check_interval: config.python_health_check_interval,
            },
            state.python.clone(),
        ))
    });
    if let Some(sidecar) = sidecar.clone() {
        state = state.with_python_sidecar(sidecar);
    }
    let skill_watcher = wire_skills(&state);

    let engine = zk_server::engine_bridge::wire_engine(&state);
    let coordinator_events = state.spawn_coordinator_event_bridge();
    let python_tasks = spawn_python_sidecar(&state, sidecar.clone());
    // Batch 4B：MCP 客户端管理器（旧 `McpClientManager` 的 `SmartLifecycle`，
    // phase 2）。句柄须在 `state` 被 `build_router` 取走之前抓取——关停时要在
    // abort 巡检任务之后调 `shutdown()`。
    let mcp = state.mcp();
    let mcp_tasks = spawn_mcp(&state, &mcp);
    // S8：WS 周期清理（TTL 过期连接 + offline 标记回收；对齐旧
    // cleanupScheduler，随进程生命周期，关停时终止）。
    let ws_cleanup = state.hub.clone().spawn_cleanup();
    // Batch 7b Step 7：Run 注册表周期清理（30min interval，滞留 run 回收）。
    let run_cleanup = engine.spawn_run_cleanup();
    // Run 启动恢复必须先于交互配额对账——对账按 run_envelopes 终态集合清
    // 孤儿 pending 交互，Run 先收敛账目才正确。
    recover_stale_runs(&state.db).await;
    // 2.5：交互生命周期常驻任务——启动期容量对账（旧 `@PostConstruct
    // reconcileCapacityAfterRestart`）+ 1s 截止扫描（旧 `@Scheduled(fixedRate=1000)
    // expireDeadlines`）+ 250ms 未 ACK 重投（旧 `@Scheduled(fixedDelay=250)`）。
    let interaction_tasks =
        zk_server::ws::spawn_interaction_lifecycle(state.hub.clone(), &state.authz.interactions)
            .await;
    let app = build_router(state);
    let (listener, bind_addr) = bind_listener(&config).await;
    tracing::info!(addr = %bind_addr, "zk-server listening");
    if let Err(err) = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    {
        eprintln!("zk-server: server error: {err}");
        std::process::exit(1);
    }
    ws_cleanup.abort();
    coordinator_events.abort();
    run_cleanup.abort();
    skill_watcher.abort();
    for task in interaction_tasks {
        task.abort();
    }
    for task in python_tasks {
        task.abort();
    }
    for task in mcp_tasks {
        task.abort();
    }
    // 旧 `SmartLifecycle.stop()` → `shutdown()`：取消重连调度、关闭全部连接。
    // 与侧车同理放在 abort 之后，避免巡检任务与关停竞争同一批连接。
    mcp.shutdown().await;
    // 侧车优雅停止（SIGTERM → 10s 宽限 → SIGKILL + 清理残留 socket 文件）；
    // 必须在 abort supervise 之后，避免停止过程被误判为崩溃而触发重启。
    if let Some(sidecar) = sidecar {
        sidecar.stop().await;
    }
    tracing::info!("zk-server stopped");
}

/// 3B.7：技能磁盘来源装配 + 热重载。内置 14 技能已随 `AppState` 编译期就位，
/// 这里只补六级磁盘来源（plugin < project < user < managed 升序注册，高优先级
/// 覆盖低优先级）并起 500ms 轮询热重载（旧 `WatchService` + 500ms 防抖的等价
/// 实现，理由见 `skill::loader` 模块文档）。扫描失败只 warn，不阻断启动。
///
/// 返回可 abort 的轮询任务句柄（关停时终止）。
fn wire_skills(state: &AppState) -> tokio::task::JoinHandle<()> {
    let skill_dirs = skill_loader::skill_dirs(
        &skill_loader::resolve_working_directory().unwrap_or_else(|| std::path::PathBuf::from(".")),
    );
    let loaded = skill_loader::load_and_register(&state.skills, &skill_dirs);
    tracing::info!(
        builtin = zk_server::skill::BUILTIN_SKILL_NAMES.len(),
        scanned = loaded.scanned,
        registered = loaded.registered,
        total = state.skills.len(),
        "skill registry ready"
    );
    skill_loader::spawn_watcher(
        state.skills.clone(),
        skill_dirs,
        skill_loader::WATCH_INTERVAL,
    )
}

/// 后台拉起 Python 侧车：启动 → 监督循环（1s liveness + 30s 健康轮询 +
/// 受限自动重启），并排入能力缓存与模型工具目录的周期同步。返回可 abort 的
/// 任务句柄集（关停时先 abort 再 stop）。
fn spawn_python_sidecar(
    state: &AppState,
    sidecar: Option<Arc<PythonSidecar>>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let Some(sidecar) = sidecar else {
        tracing::info!("python sidecar disabled; python-backed tools are not registered");
        return Vec::new();
    };
    tracing::info!(
        socket = %sidecar.socket().display(),
        health_check_interval_ms = u64::try_from(
            state.config.python_health_check_interval.as_millis()
        )
        .unwrap_or(u64::MAX),
        web_browser_tool = state.config.feature_web_browser_tool,
        git_enhanced_tool = state.config.feature_git_enhanced_tool,
        "wiring python sidecar"
    );
    let python = state.python.clone();
    let tools = state.tools();
    let browser_replay = state.browser_replay.clone();
    let web_browser_enabled = state.config.feature_web_browser_tool;
    let git_enhanced_enabled = state.config.feature_git_enhanced_tool;
    let capability_interval = state.config.python_health_check_interval;
    let observability = Arc::clone(&state.observability);
    let lifecycle = tokio::spawn(async move {
        let started = std::time::Instant::now();
        observability.record(zk_engine::ObservabilityEvent::new(
            "python",
            "sidecar_start",
            "running",
        ));
        if sidecar.start().await {
            let mut event = zk_engine::ObservabilityEvent::new("python", "sidecar_start", "ok");
            event.duration_ms =
                Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
            observability.record(event);
        } else {
            let mut event = zk_engine::ObservabilityEvent::new("python", "sidecar_start", "error");
            event.duration_ms =
                Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
            observability.record(event);
            tracing::warn!(
                "python sidecar failed to start; python-backed tools degrade gracefully"
            );
        }
        sidecar.supervise().await;
        observability.record(zk_engine::ObservabilityEvent::new(
            "python",
            "sidecar_supervise",
            "stopped",
        ));
    });
    let capability_sync = tokio::spawn(async move {
        tokio::time::sleep(zk_server::python::client::STARTUP_REFRESH_DELAY).await;
        loop {
            python.refresh_capabilities().await;
            zk_server::engine_bridge::sync_python_tool_registry(
                &tools,
                &python,
                Arc::clone(&browser_replay),
                web_browser_enabled,
                git_enhanced_enabled,
            );
            tokio::time::sleep(capability_interval).await;
        }
    });
    vec![lifecycle, capability_sync]
}

/// 后台拉起 MCP 客户端管理器：装配已在 [`AppState::mcp`] 完成（四端口 + 能力
/// 注册表 + 多来源配置解析器），这里只驱动生命周期——`start()`（旧
/// `SmartLifecycle.start()`：置 running → 初始化默认 roots → 连接三来源配置）
/// 与 30s 健康巡检（旧 `@Scheduled(fixedDelay = 30000) healthCheck()`）。
///
/// 返回可 abort 的任务句柄集（关停时先 abort 再 `shutdown()`）。
///
/// # 与旧实现的刻意差异（偏离 `B4B-17`）
///
/// 旧端在 Spring 容器刷新的 phase 2 **阻塞式**完成 `initializeAll()`；此处改为
/// 后台任务，理由与 Python 侧车一致：MCP 建连含子进程拉起与 SSE 握手，逐个
/// 串行会把「启动 <5s」判据吃掉。语义损失被限制在极小窗口内——`start()` 的
/// running 标志在 `initialize_all()` **之前**翻转，故 REST/WS 侧的
/// `MCP_CLIENT_MANAGER_NOT_RUNNING` 只可能出现在任务被调度前的一瞬。
fn spawn_mcp(
    state: &AppState,
    manager: &Arc<zk_mcp::McpClientManager>,
) -> Vec<tokio::task::JoinHandle<()>> {
    tracing::info!(
        registry_path = %state.config.mcp_registry_path.display(),
        capabilities = state.mcp_capabilities.size(),
        enabled_capabilities = state.mcp_capabilities.enabled_count(),
        trust_file = state
            .config
            .mcp_trust_file
            .as_ref()
            .map_or_else(|| "-".to_owned(), |path| path.display().to_string()),
        "wiring MCP client manager"
    );
    let boot = {
        let manager = Arc::clone(manager);
        tokio::spawn(async move {
            if let Err(error) = manager.start().await {
                // 旧端启动失败会抛出并中断容器启动；zkcode 只记错误——MCP 是
                // 增强能力，缺它不应拖死整个服务（与侧车启动失败同一处置）。
                tracing::error!(%error, "MCP client manager failed to start");
            }
        })
    };
    vec![boot, manager.spawn_health_check_loop()]
}

/// Run 启动恢复（旧 `@PostConstruct interruptStaleRuns`，
/// `RunControlService.java:321-327`）：把上次进程崩溃/重启滞留在非终态
/// （`queued` / `running` / `waiting_interaction` / `cancelling`）的 Run
/// 统一中断为 `interrupted` / `service_restart`。
async fn recover_stale_runs(db: &Db) {
    match db.interrupt_stale_runs().await {
        Ok(ids) if ids.is_empty() => {}
        // 旧源 L324/L326 的 warn + info 双日志（此处恢复为原子调用，统一在
        // 完成后打点，count 语义一致）。
        Ok(ids) => {
            tracing::warn!(
                count = ids.len(),
                "recovering stale runs after service restart"
            );
            tracing::info!(
                interrupted_count = ids.len(),
                "stale run recovery completed"
            );
        }
        // 处置与交互对账失败一致（`spawn_interaction_lifecycle` 内）：记错误
        // 不阻断监听，滞留 Run 留待下次启动重试。
        Err(error) => tracing::error!(error = %error, "stale run recovery failed"),
    }
}

/// 绑定监听套接字：地址解析失败或端口占用一律退出进程（不静默换端口——
/// macOS 本地 Beta 只接受由 [`Config`] 验证过的 loopback IP）。
async fn bind_listener(config: &Config) -> (tokio::net::TcpListener, SocketAddr) {
    let host = config
        .host
        .parse::<std::net::IpAddr>()
        .unwrap_or_else(|err| {
            eprintln!("zk-server: invalid bind address: {err}");
            std::process::exit(1);
        });
    let bind_addr = SocketAddr::new(host, config.port);
    match tokio::net::TcpListener::bind(bind_addr).await {
        Ok(listener) => (listener, bind_addr),
        Err(err) => {
            eprintln!("zk-server: cannot bind {bind_addr}: {err}");
            std::process::exit(1);
        }
    }
}

/// tracing 初始化：紧凑 JSON 结构化日志；过滤指令 `ZK_LOG` > `RUST_LOG` > `info`。
fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let directive = ["ZK_LOG", "RUST_LOG"]
        .iter()
        .find_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    let filter = directive.map_or_else(|| EnvFilter::new("info"), EnvFilter::new);
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

/// 优雅关停信号（Ctrl-C；Unix 下含 SIGTERM，供 launchd 与本地脚本停止）。
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

/// 装配多提供商注册表：`LLM_PROVIDER_*` 环境变量优先，否则 Phase 1 单
/// provider 回退（`ZK_LLM_BASE_URL` / `ZK_LLM_API_KEY`）。
///
/// 默认模型统一由 `ZK_DEFAULT_MODEL` 覆盖，`ZK_MODEL_FALLBACK_CHAIN`
/// 提供模型降级链。共享 HTTP client 构建失败（TLS 初始化异常等）时 fail
/// fast 退出——与原 S9 provider 构建失败的处置一致。
fn build_provider_registry(config: &Config) -> zk_llm::ProviderRegistry {
    let env_configs = zk_llm::provider_configs_from_env();
    let registry = if !env_configs.is_empty() {
        zk_llm::ProviderRegistry::from_configs(env_configs)
    } else if !config.llm_api_key.is_empty() {
        let phase1 = zk_llm::ProviderConfig::new(
            "openai-compat",
            config.llm_base_url.clone(),
            config.llm_api_key.clone(),
            config.default_model.clone(),
            Vec::new(),
        );
        zk_llm::ProviderRegistry::from_configs(vec![phase1])
    } else {
        Ok(zk_llm::ProviderRegistry::new())
    };
    registry
        .unwrap_or_else(|err| {
            eprintln!("zk-server: cannot initialise llm provider: {err}");
            std::process::exit(1);
        })
        .with_default_model(config.default_model.clone())
        .with_fallback_chain(zk_llm::config::fallback_chain_from_env())
}
