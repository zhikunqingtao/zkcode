//! `BashTool` 动态超时策略单元测试。
//!
//! 逐条翻译旧源 `backend/src/test/java/com/aicodeassistant/tool/bash/
//! BashTimeoutStrategyTest.java`（307 行 / 39 个 `@Test`），断言零改写。
//!
//! 旧源类注释 L10-13：验证 `BashCommandClassifier.classifyForTimeout()`
//! 能正确识别命令类型并推荐超时时间。

use zk_tools::bash::category::CommandCategory;
use zk_tools::bash::classifier::BashCommandClassifier;

/// 旧源 L18-21 `setUp()`：`new BashCommandClassifier()`。
fn classifier() -> BashCommandClassifier {
    BashCommandClassifier::new()
}

/// 旧源 `classifier.classifyForTimeout(cmd).getRecommendedTimeoutMs()` 的简写。
fn timeout_ms(cmd: Option<&str>) -> u64 {
    classifier()
        .classify_for_timeout(cmd)
        .recommended_timeout_ms()
}

// ═══════════════════════════════════════════════════════════════════
// 编译命令 → 300s —— 旧源 L23-68
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L27-31 `mvnCompile`。
#[test]
fn mvn_compile() {
    assert_eq!(300_000, timeout_ms(Some("mvn compile")));
}

/// 旧源 L33-37 `mvnwCleanPackage`。
#[test]
fn mvnw_clean_package() {
    assert_eq!(
        300_000,
        timeout_ms(Some("./mvnw clean package -DskipTests"))
    );
}

/// 旧源 L39-43 `npmRunBuild`。
#[test]
fn npm_run_build() {
    assert_eq!(300_000, timeout_ms(Some("npm run build")));
}

/// 旧源 L45-49 `cargoBuild`。
#[test]
fn cargo_build() {
    assert_eq!(300_000, timeout_ms(Some("cargo build --release")));
}

/// 旧源 L51-55 `make`。
#[test]
fn make() {
    assert_eq!(300_000, timeout_ms(Some("make -j4")));
}

/// 旧源 L57-61 `gradleBuild`。
#[test]
fn gradle_build() {
    assert_eq!(300_000, timeout_ms(Some("./gradlew build")));
}

/// 旧源 L63-67 `goBuild`。
#[test]
fn go_build() {
    assert_eq!(300_000, timeout_ms(Some("go build ./...")));
}

// ═══════════════════════════════════════════════════════════════════
// 测试命令 → 600s —— 旧源 L70-121
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L74-78 `mvnTest`。
#[test]
fn mvn_test() {
    assert_eq!(600_000, timeout_ms(Some("mvn test")));
}

/// 旧源 L80-84 `mvnwTest`。
#[test]
fn mvnw_test() {
    assert_eq!(600_000, timeout_ms(Some("./mvnw test -pl backend")));
}

/// 旧源 L86-90 `pytest`。
#[test]
fn pytest() {
    assert_eq!(600_000, timeout_ms(Some("pytest tests/")));
}

/// 旧源 L92-96 `npxJest`。
#[test]
fn npx_jest() {
    assert_eq!(600_000, timeout_ms(Some("npx jest --coverage")));
}

/// 旧源 L98-102 `npxPlaywright`。
#[test]
fn npx_playwright() {
    assert_eq!(600_000, timeout_ms(Some("npx playwright test")));
}

/// 旧源 L104-108 `cargoTest`。
#[test]
fn cargo_test() {
    assert_eq!(600_000, timeout_ms(Some("cargo test")));
}

/// 旧源 L110-114 `goTest`。
#[test]
fn go_test() {
    assert_eq!(600_000, timeout_ms(Some("go test ./...")));
}

/// 旧源 L116-120 `mvnVerify`。
#[test]
fn mvn_verify() {
    assert_eq!(600_000, timeout_ms(Some("mvn verify")));
}

// ═══════════════════════════════════════════════════════════════════
// 包安装命令 → 300s —— 旧源 L123-156
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L127-131 `npmInstall`。
#[test]
fn npm_install() {
    assert_eq!(300_000, timeout_ms(Some("npm install")));
}

/// 旧源 L133-137 `npmCi`。
#[test]
fn npm_ci() {
    assert_eq!(300_000, timeout_ms(Some("npm ci")));
}

/// 旧源 L139-143 `pipInstall`。
#[test]
fn pip_install() {
    assert_eq!(300_000, timeout_ms(Some("pip install -r requirements.txt")));
}

/// 旧源 L145-149 `yarnInstall`。
#[test]
fn yarn_install() {
    assert_eq!(300_000, timeout_ms(Some("yarn install")));
}

/// 旧源 L151-155 `mvnDependency`。
#[test]
fn mvn_dependency() {
    assert_eq!(300_000, timeout_ms(Some("mvn dependency:resolve")));
}

// ═══════════════════════════════════════════════════════════════════
// Git 操作 → 60s —— 旧源 L158-179
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L162-166 `gitStatus`。
#[test]
fn git_status() {
    assert_eq!(60_000, timeout_ms(Some("git status")));
}

/// 旧源 L168-172 `gitDiff`。
#[test]
fn git_diff() {
    assert_eq!(60_000, timeout_ms(Some("git diff --stat")));
}

/// 旧源 L174-178 `gitLog`。
#[test]
fn git_log() {
    assert_eq!(60_000, timeout_ms(Some("git log --oneline -20")));
}

// ═══════════════════════════════════════════════════════════════════
// 服务启动 → 120s —— 旧源 L181-208
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L185-189 `npmStart`。
#[test]
fn npm_start() {
    assert_eq!(120_000, timeout_ms(Some("npm start")));
}

/// 旧源 L191-195 `npmRunDev`。
#[test]
fn npm_run_dev() {
    assert_eq!(120_000, timeout_ms(Some("npm run dev")));
}

/// 旧源 L197-201 `javaJar`。
#[test]
fn java_jar() {
    assert_eq!(120_000, timeout_ms(Some("java -jar app.jar")));
}

/// 旧源 L203-207 `mvnwSpringBoot`。
#[test]
fn mvnw_spring_boot() {
    assert_eq!(120_000, timeout_ms(Some("./mvnw spring-boot:run")));
}

// ═══════════════════════════════════════════════════════════════════
// 只读命令 → 30s —— 旧源 L210-231
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L214-218 `catCommand`。
#[test]
fn cat_command() {
    assert_eq!(30_000, timeout_ms(Some("cat /etc/hosts")));
}

/// 旧源 L220-224 `lsCommand`。
#[test]
fn ls_command() {
    assert_eq!(30_000, timeout_ms(Some("ls -la")));
}

/// 旧源 L226-230 `headCommand`。
#[test]
fn head_command() {
    assert_eq!(30_000, timeout_ms(Some("head -50 file.txt")));
}

// ═══════════════════════════════════════════════════════════════════
// 搜索命令 → 60s —— 旧源 L233-254
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L237-241 `findCommand`。
#[test]
fn find_command() {
    assert_eq!(60_000, timeout_ms(Some("find . -name '*.java'")));
}

/// 旧源 L243-247 `rgCommand`。
#[test]
fn rg_command() {
    assert_eq!(60_000, timeout_ms(Some("rg pattern src/")));
}

/// 旧源 L249-253 `grepCommand`。
#[test]
fn grep_command() {
    assert_eq!(60_000, timeout_ms(Some("grep -r foo .")));
}

// ═══════════════════════════════════════════════════════════════════
// 边界情况 —— 旧源 L256-283
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L260-264 `nullCommand`。
#[test]
fn null_command() {
    assert_eq!(120_000, timeout_ms(None));
}

/// 旧源 L266-270 `emptyCommand`。
#[test]
fn empty_command() {
    assert_eq!(120_000, timeout_ms(Some("")));
}

/// 旧源 L272-276 `blankCommand`。
#[test]
fn blank_command() {
    assert_eq!(120_000, timeout_ms(Some("   ")));
}

/// 旧源 L278-282 `unknownCommand`。
#[test]
fn unknown_command() {
    assert_eq!(120_000, timeout_ms(Some("some-random-tool --flag")));
}

// ═══════════════════════════════════════════════════════════════════
// CommandCategory 枚举向后兼容 —— 旧源 L285-305
// ═══════════════════════════════════════════════════════════════════

/// 旧源 L289-296 `displayLabelPreserved`。
#[test]
fn display_label_preserved() {
    assert_eq!("read", CommandCategory::ReadOnly.display_label());
    assert_eq!("search", CommandCategory::Search.display_label());
    assert_eq!("write", CommandCategory::Modification.display_label());
    assert_eq!("info", CommandCategory::SystemInfo.display_label());
    assert_eq!("command", CommandCategory::Unknown.display_label());
}

/// 旧源 L298-304 `classifyForUIStillWorks`。
#[test]
fn classify_for_ui_still_works() {
    let c = classifier();
    assert_eq!(
        CommandCategory::Search,
        c.classify_for_ui(Some("grep -r foo ."))
    );
    assert_eq!(
        CommandCategory::ReadOnly,
        c.classify_for_ui(Some("cat file.txt"))
    );
    assert_eq!(
        CommandCategory::Modification,
        c.classify_for_ui(Some("rm -f temp"))
    );
    assert_eq!(
        CommandCategory::SystemInfo,
        c.classify_for_ui(Some("uname -a"))
    );
}
