//! Process-metadata CLI integration tests.

use std::process::Command;

#[test]
fn version_does_not_initialize_database_or_runtime() {
    let output = Command::new(env!("CARGO_BIN_EXE_zk-server"))
        .arg("--version")
        .env("ZK_DB_PATH", "/definitely/not/a/usable/database.sqlite")
        .output()
        .expect("run zk-server --version");

    assert!(output.status.success());
    let expected = format!(
        "zk-server {} (git {}, built {}, schema {}, ws {})\n",
        env!("CARGO_PKG_VERSION"),
        zk_server::BUILD_GIT_SHA,
        zk_server::BUILD_UNIX_SECONDS,
        zk_server::DB_SCHEMA_VERSION,
        zk_protocol::WS_PROTOCOL_VERSION,
    );
    assert_eq!(
        String::from_utf8(output.stdout).expect("utf8 stdout"),
        expected,
    );
    assert!(!zk_server::BUILD_GIT_SHA.trim().is_empty());
    assert!(
        zk_server::BUILD_UNIX_SECONDS.parse::<u64>().is_ok(),
        "build timestamp must be Unix seconds"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn short_version_and_help_are_side_effect_free() {
    for arg in ["-V", "--help", "-h"] {
        let output = Command::new(env!("CARGO_BIN_EXE_zk-server"))
            .arg(arg)
            .env("ZK_DB_PATH", "/definitely/not/a/usable/database.sqlite")
            .output()
            .expect("run metadata option");
        assert!(output.status.success(), "{arg}: {:?}", output.status);
        assert!(String::from_utf8_lossy(&output.stdout).contains("zk-server"));
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn non_loopback_hosts_fail_before_runtime_initialization() {
    for host in ["0.0.0.0", "192.168.1.10"] {
        let output = Command::new(env!("CARGO_BIN_EXE_zk-server"))
            .env("ZK_HOST", host)
            .env("ZK_DB_PATH", "/definitely/not/a/usable/database.sqlite")
            .output()
            .expect("run zk-server with unsupported host");

        assert!(!output.status.success(), "{host}: {:?}", output.status);
        assert!(output.stdout.is_empty(), "{host}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("macOS local Beta requires a loopback IP"),
            "{host}: {stderr}"
        );
        assert!(!stderr.contains("cannot open database"), "{host}: {stderr}");
    }
}
