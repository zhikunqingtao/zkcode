//! 统一命令黑名单服务——对照旧 `security/CommandBlacklistService.java`（全 399 行，
//! 只读权威规格），安全硬拦截层（Layer 0）。
//!
//! 三级拦截体系（旧源 L22-27）：
//! - [`BlockLevel::AbsoluteDeny`] —— 绝对禁止，任何模式下不可执行；
//! - [`BlockLevel::HighRiskAsk`] —— 高危命令，需用户确认（bypass 免疫）；
//! - [`BlockLevel::AuditLog`] —— 审计记录，不阻止执行。
//!
//! 内置规则硬编码在源码中，不可通过配置修改（旧源 L29-30）；**正则字符串逐字
//! 复用**旧源字面量，`BlockResult::rule` 回传的即为该字面量（旧源 L281
//! `rule.pattern().pattern()`）。
//!
//! 自定义规则装载（旧源 `@PostConstruct loadCustomRules()` L222-258）已逐条移植为
//! [`CommandBlacklistService::load_custom_rules`] 与
//! [`CommandBlacklistService::load_custom_rules_from_path`]：Spring `ResourceLoader`
//! 与 Jackson `readTree` 换为 `std::fs` 与 `serde_json`，字段名、缺省描述
//! `"Custom rule"`、空白 pattern 跳过、非法正则静默丢弃、整体异常降级均与旧源
//! 一致；配置文件 `resources/security-blacklist.json` 自 main 基线字节级复用。
//! 唯一差异是 `@PostConstruct` 由 Spring 容器自动触发，Rust 侧由调用方在构造后
//! 显式调用（无 `IoC` 容器）。留痕 `docs/compatibility.md` §5，分类 EQUIVALENT。

use std::sync::LazyLock;

use regex::Regex;

/// 拦截级别——对照旧源 `BlockLevel` L45-50。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockLevel {
    /// 绝对禁止。
    AbsoluteDeny,
    /// 高危，需用户确认。
    HighRiskAsk,
    /// 仅审计，不阻止。
    AuditLog,
    /// 放行。
    Allowed,
}

/// 黑名单检查结果——对照旧源 `BlockResult` L55-68。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockResult {
    /// 拦截级别。
    pub level: BlockLevel,
    /// 命中的规则（正则源串），放行时为 `None`。
    pub rule: Option<String>,
    /// 命中原因（规则描述），放行时为 `None`。
    pub reason: Option<String>,
}

impl BlockResult {
    /// 放行——对照旧源 L56-58。
    #[must_use]
    pub fn allowed() -> Self {
        Self {
            level: BlockLevel::Allowed,
            rule: None,
            reason: None,
        }
    }

    /// 绝对拒绝——对照旧源 L59-61。
    #[must_use]
    pub fn deny(rule: &str, reason: &str) -> Self {
        Self {
            level: BlockLevel::AbsoluteDeny,
            rule: Some(rule.to_owned()),
            reason: Some(reason.to_owned()),
        }
    }

    /// 高危询问——对照旧源 L62-64。
    #[must_use]
    pub fn ask(rule: &str, reason: &str) -> Self {
        Self {
            level: BlockLevel::HighRiskAsk,
            rule: Some(rule.to_owned()),
            reason: Some(reason.to_owned()),
        }
    }

    /// 审计记录——对照旧源 L65-67。
    #[must_use]
    pub fn audit(rule: &str, reason: &str) -> Self {
        Self {
            level: BlockLevel::AuditLog,
            rule: Some(rule.to_owned()),
            reason: Some(reason.to_owned()),
        }
    }
}

/// 命中 `ABSOLUTE_DENY` 时的授权错误——对照旧源 L361
/// `throw new AuthorizationException("COMMAND_ABSOLUTELY_DENIED", result.reason())`。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbsolutelyDeniedError {
    /// 错误码，恒为 `"COMMAND_ABSOLUTELY_DENIED"`。
    pub code: &'static str,
    /// 拒绝原因（规则描述）。
    pub reason: Option<String>,
}

impl std::fmt::Display for AbsolutelyDeniedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {}",
            self.code,
            self.reason.as_deref().unwrap_or_default()
        )
    }
}

impl std::error::Error for AbsolutelyDeniedError {}

/// 规则条目——对照旧源 `RuleEntry` L70。
struct RuleEntry {
    /// 编译后的正则。
    pattern: Regex,
    /// 规则描述。
    description: String,
}

impl RuleEntry {
    fn new(pattern: &str, description: &str) -> Self {
        Self {
            pattern: Regex::new(pattern).expect("static blacklist regex"),
            description: description.to_owned(),
        }
    }
}

/// 内置 `ABSOLUTE_DENY` 规则（12 条）——对照旧源 `BUILTIN_ABSOLUTE_DENY` L74-123。
static BUILTIN_ABSOLUTE_DENY: LazyLock<Vec<RuleEntry>> = LazyLock::new(|| {
    vec![
        // 递归删除系统目录
        RuleEntry::new(
            r"rm\s+(?:-[rRf]+\s+){0,5}(/|/\*|~|\$HOME)(?:\s|$)",
            "Recursive deletion of system/home directory",
        ),
        // 磁盘格式化
        RuleEntry::new(r"\bmkfs\.", "Disk formatting"),
        // 块设备直写
        RuleEntry::new(r"\bdd\s+[^\n]{0,200}of=/dev/", "Block device direct write"),
        // 块设备重定向
        RuleEntry::new(r">\s*/dev/sd[a-z]", "Block device redirection"),
        // Fork 炸弹
        RuleEntry::new(r":\(\)\{\s*:\|:&\s*\};:", "Fork bomb"),
        // 远程代码执行 (curl | sh)
        RuleEntry::new(
            r"curl\s+[^|]*\|\s*(ba)?sh",
            "Remote code execution via curl pipe",
        ),
        // 远程代码执行 (wget | sh)
        RuleEntry::new(
            r"wget\s+[^|]*\|\s*(ba)?sh",
            "Remote code execution via wget pipe",
        ),
        // 远程代码执行 (bash -c "$(curl ...)")
        RuleEntry::new(
            r#"bash\s+-c\s+["']?\$\(curl"#,
            "Remote code execution via bash -c curl",
        ),
        // 全局权限修改
        RuleEntry::new(
            r"chmod\s+(?:-R\s+)?777\s+/(?:\s|$)",
            "Global permission destruction",
        ),
        // 根目录 chown
        RuleEntry::new(
            r"chown\s+-R\s+.*\s+/\s*$",
            "Root directory ownership change",
        ),
        // 系统关机重启
        RuleEntry::new(
            r"^\s*(reboot|shutdown|halt|poweroff|init\s+[06])\b",
            "System shutdown/reboot",
        ),
        // 磁盘擦除
        RuleEntry::new(r"\b(shred|wipefs)\s+", "Irreversible data erasure"),
    ]
});

/// 内置 `HIGH_RISK_ASK` 规则（13 条）——对照旧源 `BUILTIN_HIGH_RISK_ASK` L125-178。
static BUILTIN_HIGH_RISK_ASK: LazyLock<Vec<RuleEntry>> = LazyLock::new(|| {
    vec![
        // 递归/强制删除
        RuleEntry::new(r"rm\s+(?:-[rRf]+\s+){1,5}", "Recursive/forced deletion"),
        // 危险权限
        RuleEntry::new(r"chmod\s+(?:-R\s+)?777\b", "Dangerous permission change"),
        // Git 强推
        RuleEntry::new(r"\bgit\s+push\s+.*--force", "Git force push"),
        // Git 硬重置
        RuleEntry::new(r"\bgit\s+(reset|clean)\s+--hard", "Git hard reset/clean"),
        // SQL DROP
        RuleEntry::new(r"(?i)DROP\s+(TABLE|DATABASE)", "SQL DROP operation"),
        // SQL TRUNCATE
        RuleEntry::new(r"(?i)TRUNCATE\s+TABLE", "SQL TRUNCATE operation"),
        // 强杀进程
        RuleEntry::new(r"\bkill\s+-9\s+", "Force kill process"),
        // 批量杀进程
        RuleEntry::new(r"\bkillall\s+", "Batch kill processes"),
        // 网络监听
        RuleEntry::new(r"\bnc\s+-[lp]", "Network listening"),
        // Docker 破坏
        RuleEntry::new(
            r"\bdocker\s+(rm|rmi|system\s+prune)",
            "Docker destructive operation",
        ),
        // NPM 发布
        RuleEntry::new(r"\bnpm\s+(publish|unpublish)", "NPM publish/unpublish"),
        // 特权提升命令
        RuleEntry::new(r"\b(sudo|su|doas)\s+", "Privilege escalation command"),
        // setuid/setgid 修改
        RuleEntry::new(
            r"chmod\s+(?:[^\s]*\+[su]|[^\s]*[ug]\+s)",
            "setuid/setgid permission modification",
        ),
    ]
});

/// 内置 `AUDIT_LOG` 规则（5 条）——对照旧源 `BUILTIN_AUDIT_LOG` L180-201。
static BUILTIN_AUDIT_LOG: LazyLock<Vec<RuleEntry>> = LazyLock::new(|| {
    vec![
        // 环境变量泄露
        RuleEntry::new(
            r"^\s*(env|printenv|set)\s*$",
            "Environment variable disclosure",
        ),
        // 网络请求
        RuleEntry::new(r"\b(curl|wget)\s+", "Network request"),
        // SSH 连接
        RuleEntry::new(r"\bssh\s+", "SSH connection"),
        // 包安装
        RuleEntry::new(
            r"\b(npm|pip|brew|apt|apt-get)\s+install\b",
            "Package installation",
        ),
        // Git 写操作
        RuleEntry::new(r"\bgit\s+(push|commit|merge)\b", "Git write operation"),
    ]
});

/// 统一命令黑名单服务——对照旧源 `CommandBlacklistService` L38-398。
pub struct CommandBlacklistService {
    /// 自定义 `ABSOLUTE_DENY` 规则（对照旧源 L205，默认空）。
    custom_deny_patterns: Vec<RuleEntry>,
    /// 自定义 `HIGH_RISK_ASK` 规则（对照旧源 L206，默认空）。
    custom_ask_patterns: Vec<RuleEntry>,
    /// 审计日志开关——对照旧源 L214-215
    /// `${security.enhanced-blacklist.audit-log-enabled:true}`。
    audit_log_enabled: bool,
}

impl Default for CommandBlacklistService {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandBlacklistService {
    /// 构造服务（自定义规则为空，审计开启）——对照旧源构造器 L217-220 + 默认属性值。
    #[must_use]
    pub fn new() -> Self {
        Self {
            custom_deny_patterns: Vec::new(),
            custom_ask_patterns: Vec::new(),
            audit_log_enabled: true,
        }
    }

    /// 追加自定义 `ABSOLUTE_DENY` 规则——等价旧源 `loadPatterns(root,
    /// "customDenyPatterns", ...)` L231 的运行时装载效果；非法正则静默丢弃
    /// （旧源 L252-254 `log.warn` 后跳过）。
    pub fn add_custom_deny_pattern(&mut self, pattern: &str, description: &str) {
        if let Ok(re) = Regex::new(pattern) {
            self.custom_deny_patterns.push(RuleEntry {
                pattern: re,
                description: description.to_owned(),
            });
        }
    }

    /// 追加自定义 `HIGH_RISK_ASK` 规则——等价旧源 L232。
    pub fn add_custom_ask_pattern(&mut self, pattern: &str, description: &str) {
        if let Ok(re) = Regex::new(pattern) {
            self.custom_ask_patterns.push(RuleEntry {
                pattern: re,
                description: description.to_owned(),
            });
        }
    }

    /// 设置审计日志开关——对照旧源 `@Value` 注入的 `auditLogEnabled`。
    pub fn set_audit_log_enabled(&mut self, enabled: bool) {
        self.audit_log_enabled = enabled;
    }

    /// 从 JSON 文本装载自定义规则——对照旧源 `loadCustomRules` L222-241 中
    /// `mapper.readTree(is)` 之后的两次 `loadPatterns` 调用。
    ///
    /// 逐条对齐旧源行为：JSON 解析失败整体静默降级（旧源 L238-240 catch 后
    /// `log.warn` 并保持已有规则不变）；成功则依次装载 `customDenyPatterns`、
    /// `customAskPatterns`（顺序即旧源 L231-232）。
    pub fn load_custom_rules(&mut self, json: &str) {
        let Ok(root) = serde_json::from_str::<serde_json::Value>(json) else {
            tracing::warn!(
                event = "blacklist-custom-config-invalid",
                "Failed to parse custom blacklist config"
            );
            return;
        };
        let deny = Self::load_patterns(&root, "customDenyPatterns");
        let ask = Self::load_patterns(&root, "customAskPatterns");
        let (deny_len, ask_len) = (deny.len(), ask.len());
        self.custom_deny_patterns.extend(deny);
        self.custom_ask_patterns.extend(ask);
        tracing::info!(
            event = "blacklist-custom-config-loaded",
            deny = deny_len,
            ask = ask_len,
            "Loaded custom blacklist rules"
        );
    }

    /// 从 classpath 等价路径装载自定义规则——对照旧源 `loadCustomRules`
    /// L222-241 的 `resource.exists()` 判定与整体 try/catch 静默降级。
    ///
    /// 旧源缺省路径为 `classpath:security-blacklist.json`（L211），本移植对应
    /// `crates/zk-tools/resources/security-blacklist.json`（字节级复用）。
    pub fn load_custom_rules_from_path(&mut self, path: &std::path::Path) {
        // 对照旧源 L226 `if (resource.exists())`——不存在则静默跳过。
        match std::fs::read_to_string(path) {
            Ok(text) => self.load_custom_rules(&text),
            Err(err) => {
                tracing::warn!(
                    event = "blacklist-custom-config-unreadable",
                    path = %path.display(),
                    error = %err,
                    "Failed to load custom blacklist config"
                );
            }
        }
    }

    /// 解析单个规则数组——对照旧源 `loadPatterns` L243-258。
    ///
    /// 判定顺序逐条对齐：字段缺失或非数组直接返回空；每项 `pattern` 缺失/空白
    /// 则跳过（旧源 L249）；`description` 缺省为 `"Custom rule"`（旧源 L248）；
    /// 非法正则静默丢弃（旧源 L252-254）。
    fn load_patterns(root: &serde_json::Value, field_name: &str) -> Vec<RuleEntry> {
        let mut target = Vec::new();
        let Some(arr) = root.get(field_name).and_then(serde_json::Value::as_array) else {
            return target;
        };
        for node in arr {
            let pattern = node.get("pattern").and_then(serde_json::Value::as_str);
            let desc = node
                .get("description")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("Custom rule");
            if let Some(pattern) = pattern
                && !crate::bash::javastr::java_is_blank(pattern)
            {
                match Regex::new(pattern) {
                    Ok(re) => target.push(RuleEntry {
                        pattern: re,
                        description: desc.to_owned(),
                    }),
                    Err(err) => tracing::warn!(
                        event = "blacklist-custom-pattern-invalid",
                        pattern = pattern,
                        error = %err,
                        "Invalid custom pattern"
                    ),
                }
            }
        }
        target
    }

    /// 检查命令是否命中黑名单规则——对照旧源 `checkCommand` L268-335。
    ///
    /// 判定顺序严格对齐：内置 DENY → 自定义 DENY → 内置 ASK → 自定义 ASK →
    /// 内置 AUDIT → 放行。
    #[must_use]
    pub fn check_command(&self, raw_command: &str) -> BlockResult {
        if raw_command.trim().is_empty() {
            return BlockResult::allowed();
        }

        let command = raw_command.trim();

        // 剥离常见包装前缀（/bin/rm → rm, command rm → rm）
        let stripped = Self::strip_command_prefix(command);

        // 1. ABSOLUTE_DENY — 内置规则
        for rule in BUILTIN_ABSOLUTE_DENY.iter() {
            if rule.pattern.is_match(&stripped) {
                let result = BlockResult::deny(rule.pattern.as_str(), &rule.description);
                self.log_blocked(raw_command, &result);
                return result;
            }
        }

        // 2. ABSOLUTE_DENY — 自定义规则
        for rule in &self.custom_deny_patterns {
            if rule.pattern.is_match(&stripped) {
                let result = BlockResult::deny(rule.pattern.as_str(), &rule.description);
                self.log_blocked(raw_command, &result);
                return result;
            }
        }

        // 3. HIGH_RISK_ASK — 内置规则
        for rule in BUILTIN_HIGH_RISK_ASK.iter() {
            if rule.pattern.is_match(&stripped) {
                let result = BlockResult::ask(rule.pattern.as_str(), &rule.description);
                self.log_blocked(raw_command, &result);
                return result;
            }
        }

        // 4. HIGH_RISK_ASK — 自定义规则
        for rule in &self.custom_ask_patterns {
            if rule.pattern.is_match(&stripped) {
                let result = BlockResult::ask(rule.pattern.as_str(), &rule.description);
                self.log_blocked(raw_command, &result);
                return result;
            }
        }

        // 5. AUDIT_LOG — 内置规则
        for rule in BUILTIN_AUDIT_LOG.iter() {
            if rule.pattern.is_match(&stripped) {
                let result = BlockResult::audit(rule.pattern.as_str(), &rule.description);
                if self.audit_log_enabled {
                    tracing::info!(
                        event = "command-audit",
                        command = raw_command,
                        rule = rule.description.as_str(),
                        "blacklist audit event"
                    );
                }
                return result;
            }
        }

        BlockResult::allowed()
    }

    /// 检查 argv 列表是否命中黑名单规则——对照旧源 `checkArgv` L344-350。
    ///
    /// 将 argv 以单空格合并为命令字符串后委托 [`Self::check_command`]。
    #[must_use]
    pub fn check_argv(&self, argv: &[String]) -> BlockResult {
        if argv.is_empty() {
            return BlockResult::allowed();
        }
        let reconstructed = argv.join(" ");
        self.check_command(&reconstructed)
    }

    /// 若命令属于 `ABSOLUTE_DENY`，返回错误——对照旧源
    /// `requireNotAbsolutelyDenied` L358-363。
    ///
    /// # Errors
    ///
    /// 命中绝对拒绝规则时返回 [`AbsolutelyDeniedError`]。
    pub fn require_not_absolutely_denied(
        &self,
        command: &str,
    ) -> Result<(), AbsolutelyDeniedError> {
        let result = self.check_command(command);
        if result.level == BlockLevel::AbsoluteDeny {
            return Err(AbsolutelyDeniedError {
                code: "COMMAND_ABSOLUTELY_DENIED",
                reason: result.reason,
            });
        }
        Ok(())
    }

    /// 审计落盘——等价旧源 `auditLogger.logBlocked(rawCommand, result)`。
    ///
    /// 旧源的 `SecurityAuditLogger` 为独立 Spring 组件（超出本任务安全解析器
    /// 范围），此处以 `tracing` 结构化事件承载同等信息，留痕 §5 分类 EQUIVALENT。
    fn log_blocked(&self, raw_command: &str, result: &BlockResult) {
        if self.audit_log_enabled {
            tracing::warn!(
                event = "command-blocked",
                command = raw_command,
                level = ?result.level,
                rule = result.rule.as_deref().unwrap_or_default(),
                reason = result.reason.as_deref().unwrap_or_default(),
                "blacklist blocked command"
            );
        }
    }

    /// 剥离常见命令包装前缀——对照旧源 `stripCommandPrefix` L378-398。
    ///
    /// 处理场景：`/usr/bin/rm` → `rm`、`/bin/rm` → `rm`、`command rm` → `rm`、
    /// `builtin cd` → `cd`。
    #[must_use]
    pub fn strip_command_prefix(command: &str) -> String {
        let mut stripped = command.to_owned();

        // 剥离绝对路径前缀：/usr/bin/rm -rf / → rm -rf /
        if stripped.starts_with('/') {
            let space_idx = stripped.find(' ');
            let first_token = match space_idx {
                Some(idx) => &stripped[..idx],
                None => &stripped[..],
            };
            if let Some(last_slash) = first_token.rfind('/') {
                let cmd_name = first_token[last_slash + 1..].to_owned();
                let tail = space_idx.map_or_else(String::new, |idx| stripped[idx..].to_owned());
                stripped = cmd_name + &tail;
            }
        }

        // 剥离 command/builtin 前缀
        if (stripped.starts_with("command ") || stripped.starts_with("builtin "))
            && let Some(idx) = stripped.find(' ')
        {
            stripped = stripped[idx + 1..].trim().to_owned();
        }

        stripped
    }
}
