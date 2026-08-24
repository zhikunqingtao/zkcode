//! 系统提示「环境信息」段（对照旧 `SystemPromptBuilder#computeEnvInfo`
//! L1035-1055，段落 11 `env_info`）。
//!
//! # 为什么必须注入
//!
//! 旧系统每次请求都在 system prompt 里逐行告知模型主工作目录与宿主形态。
//! zkcode 此前恒 `system_prompt=None`（D-S9-5 把整个 prompt 域推给 Phase 3），
//! 模型因此无从得知自己落在哪个目录，只能按训练习惯发出容器约定的绝对
//! `file_path`（实测 `/mnt/user-data/outputs/…`）——该路径在原生 macOS 上不
//! 存在，建父目录落到只读根卷，`Write` 报
//! `FILE_WRITE_IO_FAILED: Read-only file system (os error 30)`。
//!
//! 主工作目录本身是会话既有事实（`sessions.working_dir`，`Bash` 工具已在用
//! 同一份），本段只做「告知」：不改任何路径解析、不改授权判定——旧
//! `ManagedWorkspacePathResolver` 在授权放行后同样允许工作区外绝对路径，
//! 故把工作区外路径重写或拒绝反而是对旧行为的偏离。
//!
//! # 移植范围
//!
//! 本次只移植段落 11，其余 11 段仍属 Phase 3 引擎域。段落内逐行对齐旧源的
//! `主工作目录` / `平台` / `Shell` / `操作系统版本` / 驱动模型，并有两处不
//! 输出（宁缺勿输出不等价的值）：
//!
//! - `是否为 git 仓库`：旧源经 `GitService#isGitRepository` 落到
//!   `WorkspaceIdentityService#isValidatedGitRepositoryRoot`，除 `.git` 存在
//!   外还校验 worktree 元数据；该服务在 zk-authz，而依赖铁律禁止
//!   `zk-engine → zk-authz`，以「`.git` 是否存在」近似会得到与旧源不同的判定。
//! - `额外工作目录`：zkcode 会话只有单一工作目录，无旧 `additionalDirs` 概念。

/// 段落标题（旧源逐字 `"# 环境"`）。
const HEADING: &str = "# 环境";

/// `SHELL` 缺省占位（旧源 `System.getenv("SHELL") != null ? … : "unknown"`）。
const UNKNOWN_SHELL: &str = "unknown";

/// 模型缺省占位（旧源 `model != null ? model : "default"`）。
const DEFAULT_MODEL: &str = "default";

/// 构造「环境信息」段落文本（末行带换行，形状对齐旧 `computeEnvInfo` 的
/// `StringBuilder`）。
///
/// `working_dir` 取会话的 `sessions.working_dir`；空白时回落进程当前目录
/// （旧源 `workingDir != null ? workingDir : System.getProperty("user.dir")`）。
#[must_use]
pub fn environment_section(working_dir: &str, model: &str) -> String {
    let (platform, os_version) = os_identity();
    let shell = std::env::var("SHELL").unwrap_or_else(|_| UNKNOWN_SHELL.to_owned());
    let model = if model.trim().is_empty() {
        DEFAULT_MODEL
    } else {
        model
    };
    format!(
        "{HEADING}\n\
         你已在以下环境中被调用：\n\
         \x20- 主工作目录：{cwd}\n\
         \x20- 平台：{platform}\n\
         \x20- Shell：{shell}\n\
         \x20- 操作系统版本：{os_version}\n\
         \x20- 你由模型 {model} 驱动。\n",
        cwd = effective_working_dir(working_dir),
    )
}

/// 主工作目录。会话工作目录在创建期已由 zk-server `canonicalize_for_create`
/// 归一为绝对真实路径，故此处不再触盘归一，只处理缺省回落。
/// （[`crate::system_prompt::scratchpad_section`] 复用同一回落规则，保持两段同源。）
pub(crate) fn effective_working_dir(working_dir: &str) -> String {
    let configured = working_dir.trim();
    if !configured.is_empty() {
        return configured.to_owned();
    }
    std::env::current_dir().map_or_else(
        |_| String::new(),
        |path| path.to_string_lossy().into_owned(),
    )
}

/// 宿主标识 `(平台, 操作系统版本)`。旧源读 JVM 的 `os.name` / `os.version`，
/// Rust 无等价属性，取 `uname(2)`：`sysname` 对位 `os.name`、`release` 对位
/// `os.version`，版本行同旧源为「名 + 空格 + 版本」。
fn os_identity() -> (String, String) {
    let Ok(uts) = nix::sys::utsname::uname() else {
        // uname 失败（理论不可达）：回落编译期目标名，保证段落形状不变。
        let target = std::env::consts::OS.to_owned();
        return (target.clone(), target);
    };
    let sysname = uts.sysname().to_string_lossy().into_owned();
    let release = uts.release().to_string_lossy();
    let version = if release.is_empty() {
        sysname.clone()
    } else {
        format!("{sysname} {release}")
    };
    (sysname, version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn section_states_configured_working_directory_and_model() {
        let section = environment_section("/Users/dev/project", "kimi-k3");
        let lines: Vec<&str> = section.lines().collect();
        assert_eq!(lines[0], "# 环境");
        assert_eq!(lines[1], "你已在以下环境中被调用：");
        assert_eq!(lines[2], " - 主工作目录：/Users/dev/project");
        assert!(lines[3].starts_with(" - 平台："));
        assert!(lines[4].starts_with(" - Shell："));
        assert!(lines[5].starts_with(" - 操作系统版本："));
        assert_eq!(lines[6], " - 你由模型 kimi-k3 驱动。");
        assert_eq!(lines.len(), 7);
        assert!(section.ends_with("驱动。\n"));
    }

    #[test]
    fn blank_working_directory_falls_back_to_process_cwd() {
        let expected = std::env::current_dir()
            .expect("cwd")
            .to_string_lossy()
            .into_owned();
        for raw in ["", "   "] {
            let section = environment_section(raw, "m");
            assert!(
                section.contains(&format!(" - 主工作目录：{expected}\n")),
                "{section}"
            );
        }
    }

    #[test]
    fn blank_model_falls_back_to_default_placeholder() {
        let section = environment_section("/tmp", "  ");
        assert!(
            section.contains(" - 你由模型 default 驱动。\n"),
            "{section}"
        );
    }

    /// 平台 / 版本必须是真实宿主取值（非空且版本以平台名起头）。
    #[test]
    fn os_identity_reports_non_empty_host_values() {
        let (platform, version) = os_identity();
        assert!(!platform.is_empty());
        assert!(version.starts_with(&platform), "{version} / {platform}");
    }
}
