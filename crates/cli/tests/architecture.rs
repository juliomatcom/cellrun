//! Guards the Clean Architecture dependency rule: inner circles never depend
//! on outer ones. Cargo already refuses imports of crates that are not
//! declared; this test refuses the declarations themselves.

use std::path::Path;

/// (crate, forbidden runtime dependencies)
const RULES: &[(&str, &[&str])] = &[
    (
        "domain",
        &[
            "application",
            "infrastructure",
            "cli",
            "bollard",
            "tokio",
            "serde",
            "serde_json",
            "futures-util",
        ],
    ),
    (
        "application",
        &[
            "infrastructure",
            "cli",
            "bollard",
            "tokio",
            "serde",
            "serde_json",
        ],
    ),
    ("infrastructure", &["cli", "clap"]),
];

/// Names declared under `[dependencies]` (dev-dependencies are allowed to
/// pull test helpers such as tokio).
fn runtime_dependencies(manifest: &str) -> Vec<String> {
    let mut in_deps = false;
    let mut deps = Vec::new();
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            in_deps = line == "[dependencies]";
            if let Some(name) = line.strip_prefix("[dependencies.") {
                deps.push(name.trim_end_matches(']').to_owned());
            }
            continue;
        }
        if in_deps && !line.is_empty() && !line.starts_with('#') {
            if let Some((key, _)) = line.split_once('=') {
                let key = key.trim().trim_end_matches(".workspace").trim_matches('"');
                deps.push(key.to_owned());
            }
        }
    }
    deps
}

#[test]
fn inner_circles_do_not_depend_on_outer_circles() {
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    for (krate, forbidden) in RULES {
        let manifest = std::fs::read_to_string(crates_dir.join(krate).join("Cargo.toml")).unwrap();
        let deps = runtime_dependencies(&manifest);
        for dep in &deps {
            assert!(
                !forbidden.contains(&dep.as_str()),
                "dependency rule violated: `{krate}` must not depend on `{dep}`"
            );
        }
    }
}

#[test]
fn parser_reads_dependency_names() {
    let manifest = "[package]\nname = \"x\"\n[dependencies]\ndomain.workspace = true\n\
                    serde = { version = \"1\" }\n# comment\n[dev-dependencies]\ntokio = \"1\"\n\
                    [dependencies.bollard]\nversion = \"0.21\"\n";
    assert_eq!(
        runtime_dependencies(manifest),
        ["domain", "serde", "bollard"]
    );
}
