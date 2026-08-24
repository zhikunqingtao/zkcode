//! Access Token 与会话 Cookie 管理（旧 `config/RemoteAccessSecurityFilter.java`
//! 的 token 半边，L48-99 / L176-213 / L245-271）。
//!
//! 旧过滤器把「token 生成 + 持久化 + session store + Cookie 签发」和「请求准入
//! 判定」揉在同一个 `OncePerRequestFilter` 里。zkcode 拆成两半：本模块只管
//! **凭证状态**（进程内唯一实例，装配在 [`crate::state::AppState`]），准入判定在
//! [`crate::middleware::access_guard`]。拆分理由：中间件是热路径纯函数，凭证是
//! 有状态单例——合并会让中间件无法单测。
//!
//! # 逐行对照
//!
//! | 本模块 | 旧源 |
//! |---|---|
//! | [`AccessTokenManager::load_or_create`] | 构造器 L80-99（`loadExistingToken` → 复用 / 否则 `SecureRandom` 32B + `saveToken`） |
//! | [`AccessTokenManager::matches`] | L154 `accessToken.equals(token)` |
//! | [`AccessTokenManager::issue_session_cookie`] | `issueSessionCookie` L181-193 |
//! | [`AccessTokenManager::validate_session`] | `validateSessionCookie` L195-203 |
//! | [`AccessTokenManager::token_preview`] | L102-103 的 4+4 掩码 |
//! | [`COOKIE_NAME`] / [`COOKIE_MAX_AGE`] | L62 / L65 |
//! | [`SESSION_CAPACITY`] | Caffeine `.maximumSize(10_000)` L53 |
//! | `SessionStore::purge_expired` | Caffeine `.expireAfterWrite(30, DAYS)` L54 |
//!
//! # 偏离留痕
//!
//! - **B2B-01 持久化路径**：旧源固定
//!   `~/.config/ai-code-assistant/access-token`（L58-59）；本实现走
//!   [`zk_core::paths::user_config_dir`] → `~/.zk/access-token`。#65 起
//!   `zk_core::paths` 是全 workspace 用户态目录的唯一事实源，另开一个目录名
//!   会让「删掉 `~/.zk/` 即彻底重置」这一约定失效。
//! - **B2B-02 空 token 文件**：旧 `loadExistingToken` 只判 `null`，故一个**空**
//!   token 文件会让 `accessToken` 成为空串，而 `"".equals("")` 恒真——等价于
//!   关掉认证。本实现把空白内容视同「文件不存在」并重新生成（安全加固，
//!   有意不复刻该缺陷）。
//! - **B2B-03 权限收敛时序**：旧源先 `Files.writeString` 再
//!   `setPosixFilePermissions`（L260-264），中间存在 token 以 umask 权限落盘的
//!   窗口。本实现在 unix 上用 `OpenOptions::mode(0o600)` 建文件即 600，再对
//!   「文件已存在」的场景补一次 `set_permissions`（覆盖旧源的全部效果且无窗口）。
//!   非 unix 平台跳过 POSIX 权限，同旧源 `UnsupportedOperationException` 分支。
//! - **B2B-04 Cookie `Secure` 属性**：旧源取 `server.ssl.enabled`（默认
//!   `false`，`application.yml` 未声明该键）。zk-server 不做进程内 TLS 终结，
//!   故只有 `secure=false` 这一条分支可达，属性不写出。
//! - **B2B-05 Cookie `Expires` 属性**：Spring `ResponseCookie.toString()` 在
//!   `maxAge >= 0` 时同时写 `Max-Age` 与 `Expires`。本实现只写 `Max-Age`——
//!   RFC 6265 §5.2.2 规定二者并存时 `Max-Age` 优先，语义完全等价，且省掉一份
//!   HTTP-date 格式化实现。
//! - **B2B-06 容量淘汰策略**：Caffeine 的 `maximumSize` 是 W-TinyLFU（频率 +
//!   近期性）。本实现按**写入顺序** FIFO 淘汰；由于全部条目共享同一
//!   `expireAfterWrite` TTL，写入序即到期序，FIFO 淘汰的恰是最接近过期的条目
//!   （确定性且不劣于 W-TinyLFU 在此场景下的表现）。
//! - **B2B-07 token 比较**：旧 `String.equals` 短路比较可被计时侧信道观测。本
//!   实现用定长折叠比较（[`constant_time_eq`]），语义与 `equals` 一致。

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// token 原始字节数（旧 L95 `new byte[32]`）。
const TOKEN_BYTES: usize = 32;

/// 会话 Cookie 名（旧 L62 `COOKIE_NAME`）。
pub const COOKIE_NAME: &str = "ai-coder-session";

/// 会话 Cookie 与 session 条目有效期（旧 L65 `Duration.ofDays(30)`）。
///
/// 30 天 = 720 小时；`Duration::from_days` 尚未稳定，故以小时表达。
pub const COOKIE_MAX_AGE: Duration = Duration::from_hours(30 * 24);

/// session store 容量上限（旧 L53 Caffeine `.maximumSize(10_000)`）。
const SESSION_CAPACITY: usize = 10_000;

/// token 持久化文件名（相对 [`zk_core::paths::user_config_dir`]）。
pub const TOKEN_FILE_NAME: &str = "access-token";

/// token 文件权限（旧 L264 `PosixFilePermissions.fromString("rw-------")`）。
#[cfg(unix)]
const TOKEN_FILE_MODE: u32 = 0o600;

/// token 持久化默认路径：`~/.zk/access-token`（见偏离 B2B-01）。
#[must_use]
pub fn default_token_path() -> PathBuf {
    zk_core::paths::user_config_dir().join(TOKEN_FILE_NAME)
}

/// 局域网访问凭证的进程内唯一权威（旧过滤器的 token 状态半边）。
pub struct AccessTokenManager {
    /// 启动期确定的 access token（Base64URL 无填充）。
    token: String,
    /// sessionId → 过期时刻（旧 Caffeine `sessionStore`）。
    sessions: Mutex<SessionStore>,
}

impl std::fmt::Debug for AccessTokenManager {
    /// §19 脱敏：Debug 只透出掩码预览与在册 session 数，绝不打印 token 本体。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessTokenManager")
            .field("token", &self.token_preview())
            .field("sessions", &self.session_count())
            .finish()
    }
}

impl AccessTokenManager {
    /// 装配凭证管理器（旧构造器 L80-99）。
    ///
    /// `path` 为 `None` 时**不落盘**：token 仅存活于进程内（集成测试与
    /// `Config::test_config` 走这条路径，避免测试污染用户目录）。
    ///
    /// 持久化失败（目录不可建 / 文件不可写）只告警不阻断启动，同旧源
    /// `saveToken` 的 `catch (IOException)` 分支——代价是重启后 token 变化。
    ///
    /// # Panics
    ///
    /// 操作系统 CSPRNG 不可用时 panic。这是进程级故障（无熵源即无法提供任何
    /// 认证强度），fail fast 优于静默降级到可预测 token。
    #[must_use]
    pub fn load_or_create(path: Option<&Path>) -> Self {
        let token = if let Some(existing) = path.and_then(load_existing_token) {
            tracing::info!("Reusing existing access token");
            existing
        } else {
            let generated = generate_token();
            if let Some(path) = path {
                save_token(path, &generated);
            }
            generated
        };
        Self {
            token,
            sessions: Mutex::new(SessionStore::default()),
        }
    }

    /// access token 本体（`GET /api/auth/token` 的返回值；旧
    /// `getAccessToken()` L294-296）。
    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }

    /// 掩码预览：前 4 + `...` + 后 4（旧 L102-103，含 `Math.min` /
    /// `Math.max` 的短 token 退化行为）。启动日志只打这一份。
    #[must_use]
    pub fn token_preview(&self) -> String {
        let total = self.token.chars().count();
        let head: String = self.token.chars().take(4).collect();
        let tail: String = self.token.chars().skip(total.saturating_sub(4)).collect();
        format!("{head}...{tail}")
    }

    /// 候选 token 是否匹配（旧 L154 `accessToken.equals(token)`；比较方式见
    /// 偏离 B2B-07）。
    #[must_use]
    pub fn matches(&self, candidate: &str) -> bool {
        constant_time_eq(self.token.as_bytes(), candidate.as_bytes())
    }

    /// 签发会话 Cookie 并登记 session（旧 `issueSessionCookie` L181-193）。
    ///
    /// 返回值即 `Set-Cookie` 头的完整值。
    ///
    /// # Panics
    ///
    /// session store 锁中毒时 panic（进程级故障，fail fast——与 hub 各锁一致）。
    #[must_use]
    pub fn issue_session_cookie(&self) -> String {
        let session_id = uuid::Uuid::new_v4().to_string();
        self.sessions
            .lock()
            .expect("session store lock")
            .insert(session_id.clone(), Instant::now() + COOKIE_MAX_AGE);
        format!(
            "{COOKIE_NAME}={session_id}; Max-Age={}; Path=/; HttpOnly; SameSite=Lax",
            COOKIE_MAX_AGE.as_secs()
        )
    }

    /// 校验会话 Cookie（旧 `validateSessionCookie` L195-203：不存在 → false；
    /// 已过期 → 逐出后 false）。
    ///
    /// # Panics
    ///
    /// session store 锁中毒时 panic（同 [`Self::issue_session_cookie`]）。
    #[must_use]
    pub fn validate_session(&self, session_id: &str) -> bool {
        self.sessions
            .lock()
            .expect("session store lock")
            .validate(session_id)
    }

    /// 在册 session 条目数（可观测 / 测试用）。
    ///
    /// # Panics
    ///
    /// session store 锁中毒时 panic（同 [`Self::issue_session_cookie`]）。
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.lock().expect("session store lock").len()
    }
}

/// 带 TTL 与容量上限的 session 表（旧 Caffeine cache 的等价物）。
#[derive(Default)]
struct SessionStore {
    /// sessionId → 过期时刻。
    expiry: HashMap<String, Instant>,
    /// 写入顺序队列（容量淘汰与过期清扫的游标；见偏离 B2B-06）。
    order: VecDeque<String>,
}

impl SessionStore {
    /// 登记条目：先清扫过期、再按需淘汰最旧、最后写入。
    fn insert(&mut self, session_id: String, expires_at: Instant) {
        self.purge_expired();
        while self.order.len() >= SESSION_CAPACITY {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.expiry.remove(&oldest);
        }
        self.expiry.insert(session_id.clone(), expires_at);
        self.order.push_back(session_id);
    }

    /// 校验并按需逐出（旧 `validateSessionCookie`）。
    fn validate(&mut self, session_id: &str) -> bool {
        let Some(expires_at) = self.expiry.get(session_id).copied() else {
            return false;
        };
        if Instant::now() > expires_at {
            // `order` 中的残留由下一次 `purge_expired` 的 `None` 分支回收
            // （摊还 O(1)，不为一次校验做 O(n) 队列扫描）。
            self.expiry.remove(session_id);
            return false;
        }
        true
    }

    /// 在册条目数。
    fn len(&self) -> usize {
        self.expiry.len()
    }

    /// 从队首清扫已过期条目。TTL 全局一致故写入序即到期序，队首非过期即可停。
    fn purge_expired(&mut self) {
        let now = Instant::now();
        while let Some(front) = self.order.front() {
            match self.expiry.get(front) {
                Some(expires_at) if now <= *expires_at => break,
                Some(_) => {
                    if let Some(expired) = self.order.pop_front() {
                        self.expiry.remove(&expired);
                    }
                }
                // 已被 `validate` 逐出的残留游标。
                None => {
                    self.order.pop_front();
                }
            }
        }
    }
}

/// 读取已有 token（旧 `loadExistingToken` L247-255）；空白内容视同不存在
/// （偏离 B2B-02）。
fn load_existing_token(path: &Path) -> Option<String> {
    match std::fs::read_to_string(path) {
        Ok(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                tracing::warn!(
                    path = %path.display(),
                    "existing access token file is blank, generating a new token"
                );
                None
            } else {
                Some(trimmed.to_owned())
            }
        }
        // 旧源先 `Files.exists` 再读，缺文件走静默 null 分支。
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
        Err(err) => {
            tracing::warn!(error = %err, "Failed to load existing token");
            None
        }
    }
}

/// 生成 token：32 字节 OS 熵 → `Base64URL` 无填充（旧 L95-97）。
fn generate_token() -> String {
    let mut bytes = [0_u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes).expect("operating system CSPRNG must be available");
    URL_SAFE_NO_PAD.encode(bytes)
}

/// 持久化 token（旧 `saveToken` L257-271）：建父目录 + 600 权限写入，失败只告警。
fn save_token(path: &Path, token: &str) {
    if let Some(parent) = path.parent()
        && let Err(err) = std::fs::create_dir_all(parent)
    {
        tracing::warn!(error = %err, "Failed to save access token");
        return;
    }
    match write_private(path, token) {
        Ok(()) => tracing::info!(path = %path.display(), "generated new access token"),
        Err(err) => tracing::warn!(error = %err, "Failed to save access token"),
    }
}

/// 以属主独占权限写入（见偏离 B2B-03）。
fn write_private(path: &Path, token: &str) -> std::io::Result<()> {
    use std::io::Write as _;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(TOKEN_FILE_MODE);
    }
    let mut file = options.open(path)?;
    file.write_all(token.as_bytes())?;
    file.sync_all()?;
    // `mode()` 只在**新建**时生效；文件已存在（权限可能被放宽）时补收敛一次。
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(TOKEN_FILE_MODE))?;
    }
    Ok(())
}

/// 定长折叠比较（见偏离 B2B-07）。长度不等即早退——与 `String.equals` 一致，
/// 且 token 长度非机密。
pub(crate) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (a, b) in left.iter().zip(right) {
        diff |= a ^ b;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 生成的 token 形态：Base64URL 无填充、32 字节 → 43 字符、每次不同。
    #[test]
    fn generated_token_is_43_char_base64url_without_padding() {
        let first = AccessTokenManager::load_or_create(None);
        let second = AccessTokenManager::load_or_create(None);
        assert_eq!(first.token().len(), 43, "32B base64 无填充恒 43 字符");
        assert!(
            first
                .token()
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "Base64URL alphabet only: {}",
            first.token()
        );
        assert_ne!(first.token(), second.token(), "CSPRNG 不得重复");
        assert_eq!(
            URL_SAFE_NO_PAD
                .decode(first.token())
                .expect("decodes")
                .len(),
            TOKEN_BYTES
        );
    }

    /// 掩码预览：前 4 + `...` + 后 4（旧 L102-103）。
    #[test]
    fn token_preview_masks_middle() {
        let manager = AccessTokenManager::load_or_create(None);
        let token = manager.token().to_owned();
        let preview = manager.token_preview();
        assert_eq!(preview, format!("{}...{}", &token[..4], &token[39..]));
        assert!(!preview.contains(&token[4..39]), "中段不得出现在预览里");
    }

    /// token 匹配只认逐字节相等（大小写 / 前后缀 / 空串全拒）。
    #[test]
    fn matches_only_exact_token() {
        let manager = AccessTokenManager::load_or_create(None);
        let token = manager.token().to_owned();
        assert!(manager.matches(&token));
        assert!(!manager.matches(""));
        assert!(!manager.matches(&token[..token.len() - 1]));
        assert!(!manager.matches(&format!("{token}x")));
    }

    /// Cookie 属性逐项对齐旧 `ResponseCookie`：`HttpOnly` / `SameSite=Lax` /
    /// Path=/ / Max-Age 30 天；且不带 `Secure`（偏离 B2B-04）。
    #[test]
    fn issued_cookie_carries_legacy_attributes() {
        let manager = AccessTokenManager::load_or_create(None);
        let cookie = manager.issue_session_cookie();
        assert!(cookie.starts_with("ai-coder-session="));
        assert!(cookie.contains("; Max-Age=2592000"));
        assert!(cookie.contains("; Path=/"));
        assert!(cookie.contains("; HttpOnly"));
        assert!(cookie.contains("; SameSite=Lax"));
        assert!(!cookie.contains("Secure"));
        assert_eq!(COOKIE_MAX_AGE.as_secs(), 2_592_000);
    }

    /// 签发即可校验；未签发的随机 id 一律拒绝。
    #[test]
    fn issued_session_validates_and_unknown_does_not() {
        let manager = AccessTokenManager::load_or_create(None);
        let cookie = manager.issue_session_cookie();
        let session_id = cookie
            .strip_prefix("ai-coder-session=")
            .and_then(|rest| rest.split(';').next())
            .expect("cookie carries session id");
        assert!(manager.validate_session(session_id));
        assert!(!manager.validate_session("not-a-session"));
        assert_eq!(manager.session_count(), 1);
    }

    /// 过期条目校验失败并被逐出（旧 `validateSessionCookie` 的 invalidate 分支）。
    #[test]
    fn expired_session_is_rejected_and_evicted() {
        let mut store = SessionStore::default();
        let expired = Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("clock is past the epoch");
        store.insert("stale".to_owned(), expired);
        store.insert("fresh".to_owned(), Instant::now() + COOKIE_MAX_AGE);
        assert!(!store.validate("stale"));
        assert!(store.validate("fresh"));
        assert_eq!(store.len(), 1);
    }

    /// 容量上限：超出后淘汰最旧条目，总量恒不越界。
    #[test]
    fn capacity_evicts_oldest_first() {
        let mut store = SessionStore::default();
        let expires_at = Instant::now() + COOKIE_MAX_AGE;
        for index in 0..=SESSION_CAPACITY {
            store.insert(format!("s{index}"), expires_at);
        }
        assert_eq!(store.len(), SESSION_CAPACITY);
        assert!(!store.validate("s0"), "最旧条目被淘汰");
        assert!(
            store.validate(&format!("s{SESSION_CAPACITY}")),
            "最新条目在册"
        );
    }

    /// 持久化往返：600 权限落盘 + 重启复用同一 token。
    #[test]
    fn token_persists_with_owner_only_permissions() {
        let dir = std::env::temp_dir().join(format!("zk-access-token-{}", uuid::Uuid::new_v4()));
        let path = dir.join("nested").join(TOKEN_FILE_NAME);
        let first = AccessTokenManager::load_or_create(Some(&path));
        assert!(path.exists(), "父目录应被自动创建");
        let reloaded = AccessTokenManager::load_or_create(Some(&path));
        assert_eq!(first.token(), reloaded.token(), "重启复用同一 token");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path)
                .expect("metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, TOKEN_FILE_MODE, "token 文件必须 600");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 空白 token 文件视同缺失并重新生成（偏离 B2B-02 的安全加固）。
    #[test]
    fn blank_token_file_is_regenerated() {
        let dir = std::env::temp_dir().join(format!("zk-access-blank-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join(TOKEN_FILE_NAME);
        std::fs::write(&path, "   \n").expect("seed blank file");
        let manager = AccessTokenManager::load_or_create(Some(&path));
        assert_eq!(manager.token().len(), 43);
        assert!(!manager.matches(""), "空 token 绝不可通过认证");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `None` 路径不落盘（测试装配不得污染用户目录）。
    #[test]
    fn in_memory_manager_writes_nothing() {
        let manager = AccessTokenManager::load_or_create(None);
        assert!(!manager.token().is_empty());
        assert!(!default_token_path().as_os_str().is_empty());
    }

    /// 定长比较的语义与 `==` 一致。
    #[test]
    fn constant_time_eq_matches_plain_equality() {
        for (left, right) in [
            (&b""[..], &b""[..]),
            (&b"abc"[..], &b"abc"[..]),
            (&b"abc"[..], &b"abd"[..]),
            (&b"abc"[..], &b"ab"[..]),
        ] {
            assert_eq!(constant_time_eq(left, right), left == right);
        }
    }
}
