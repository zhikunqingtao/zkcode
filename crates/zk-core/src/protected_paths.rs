//! Shared recursive-search exclusion names and authorization policy inputs.

/// Credential and executable-configuration file names excluded from broad searches.
pub const DANGEROUS_FILES: &[&str] = &[
    ".gitconfig",
    ".gitmodules",
    ".bashrc",
    ".bash_profile",
    ".bash_login",
    ".bash_logout",
    ".zshrc",
    ".zprofile",
    ".zshenv",
    ".zlogin",
    ".profile",
    ".login",
    ".ripgreprc",
    ".env",
    ".env.local",
    ".env.production",
    ".mcp.json",
    crate::paths::CONFIG_FILE_NAME,
    // 遗留保护面（#65）：迁移是**拷贝**而非移动，旧配置文件仍留在盘上，
    // 把它从名单里删掉等于让旧文件失去授权门禁——保护面只许单调扩张。
    crate::paths::LEGACY_CONFIG_FILE_NAME,
    ".npmrc",
    ".yarnrc",
    "id_rsa",
    "id_ed25519",
    "id_ecdsa",
    "known_hosts",
    "authorized_keys",
    ".pgpass",
    ".my.cnf",
    ".netrc",
    ".curlrc",
    "credentials",
    "token.json",
];

// ===== Layer 2: 危险目录黑名单 =====
// 对照 `PathSecurityService.java:68-74`。
/// Control, credential, IDE and dependency directory names excluded below search roots.
pub const DANGEROUS_DIRECTORIES: &[&str] = &[
    ".git",
    ".vscode",
    ".idea",
    crate::paths::CONFIG_DIR_NAME,
    // 遗留保护面（#65），理由同 `DANGEROUS_FILES`。
    crate::paths::LEGACY_CONFIG_DIR_NAME,
    ".ai-code-assistant",
    ".ssh",
    ".gnupg",
    ".aws",
    ".config",
    ".local",
    ".kube",
    ".docker",
    "node_modules",
];
