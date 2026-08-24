//! zkcode 基础设施层——路径解析、目录迁移、特性标志。
//!
//! 本 crate 是 workspace 最底层依赖（只出不入），零 async/零 serde，
//! 仅提供纯同步基础设施供所有上层 crate 消费。
//!
//! # 目录约定
//!
//! 用户态与项目态状态统一落在 [`paths::CONFIG_DIR_NAME`]（`.zk`）下：
//! `~/.zk/` 为用户全局根，`{cwd}/.zk/` 为项目根，`{cwd}/.zk/scratchpad/` 为
//! 工作区暂存区。旧版布局由 [`migrate::run_if_needed`] 在进程启动期一次性
//! 迁移；其目录名是全 workspace **唯一**的字面量定义
//! （[`paths::LEGACY_CONFIG_DIR_NAME`]）——上层 crate 若需引用旧布局
//! （例如授权门禁对旧目录的遗留保护面）必须引用该常量，禁止再次硬编码。
//!
//! # 特性标志
//!
//! [`feature_flags::FeatureFlags`] 是应用级开关的唯一事实源（旧
//! `FeatureFlagService` 单例 Bean）：出厂默认值逐字对齐旧 `application.yml` 的
//! `features.flags` 节，环境变量覆盖优先。装配一次、以 `Arc` 跨层共享，flag 名
//! 一律引用本模块常量（如 [`feature_flags::WEB_BROWSER_TOOL`]），禁止各处硬编码
//! 字符串。

pub mod feature_flags;
pub mod migrate;
pub mod paths;

pub use feature_flags::{FeatureFlags, FlagValue};
