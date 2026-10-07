//! Real native Bash -> Python -> deterministic Office artifact validation.
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use zk_tools::{BashTool, Tool, ToolContext, ToolOutput};

fn quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}

async fn execute(root: &Path, command: String) -> ToolOutput {
    let (tx, _rx) = mpsc::unbounded_channel();
    let context = ToolContext::new(CancellationToken::new(), tx).with_working_dir(root);
    BashTool
        .execute(
            serde_json::json!({"command":command,"timeout":30000}),
            context,
        )
        .await
}

#[tokio::test]
async fn real_bash_generates_and_checks_positive_and_negative_office_artifacts() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let bundled = root.join("python-service/.venv/bin/python");
    let python = if bundled.is_file() {
        bundled
    } else {
        PathBuf::from("python3")
    };
    let output = std::env::temp_dir().join(format!("zk-office-bash-{}", uuid::Uuid::new_v4()));
    let script = root.join("tools/office-regression/scripts/office_fixture_gen.py");
    let generated = execute(
        &root,
        format!(
            "{} {} --out {}",
            quote(&python),
            quote(&script),
            quote(&output)
        ),
    )
    .await;
    assert!(!generated.is_error, "{}", generated.content);
    assert!(output.join("xlsx/positive.xlsx").is_file());
    assert!(output.join("xlsx/negative-missing-formula.xlsx").is_file());
    let checker = root.join("tools/office-regression/scripts/office_struct_check.py");
    for (file, passed, code) in [
        ("positive.xlsx", true, "\"ok\": true"),
        (
            "negative-missing-formula.xlsx",
            false,
            "XLSX_MISSING_FORMULA",
        ),
    ] {
        let validation = execute(
            &root,
            format!(
                "{} {} structure --kind xlsx --file {} --spec {}",
                quote(&python),
                quote(&checker),
                quote(&output.join("xlsx").join(file)),
                quote(&output.join("specs/xlsx.json"))
            ),
        )
        .await;
        assert_eq!(!validation.is_error, passed, "{}", validation.content);
        assert!(validation.content.contains(code), "{}", validation.content);
        assert_eq!(
            validation.metadata.as_ref().unwrap()["structuredResult"]["exitCode"],
            if passed { 0 } else { 2 }
        );
    }
    std::fs::remove_dir_all(output).unwrap();
}
