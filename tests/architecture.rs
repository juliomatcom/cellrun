//! Guards the Clean Architecture dependency rule: inner layers never depend
//! on outer ones. Layers are modules of one crate, so the rule is checked on
//! the sources: which crate-internal layers and external crates each layer's
//! non-test code names.

use std::fs;
use std::path::{Path, PathBuf};

/// (layer directory, forbidden path prefixes)
const RULES: &[(&str, &[&str])] = &[
    (
        "domain",
        &[
            "crate::application",
            "crate::infrastructure",
            "bollard",
            "tokio",
            "serde",
            "serde_json",
            "futures_util",
            "clap",
        ],
    ),
    (
        "application",
        &[
            "crate::infrastructure",
            "bollard",
            "tokio",
            "serde",
            "serde_json",
            "clap",
        ],
    ),
    ("infrastructure", &["clap"]),
];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Paths named by `use` declarations, outside `#[cfg(test)]` modules
/// (tests may pull helpers such as tokio).
fn used_paths(source: &str) -> Vec<String> {
    let production = source.split("#[cfg(test)]").next().unwrap_or_default();
    let mut paths = Vec::new();
    let mut stmt = String::new();
    for line in production.lines().map(str::trim) {
        if stmt.is_empty() && !(line.starts_with("use ") || line.starts_with("pub use ")) {
            continue;
        }
        stmt.push_str(line);
        if line.ends_with(';') {
            let body = stmt.trim_start_matches("pub ").trim_start_matches("use ");
            paths.push(body.trim_end_matches(';').replace(' ', ""));
            stmt.clear();
        }
    }
    paths
}

fn violates(path: &str, forbidden: &str) -> bool {
    // `crate::x` is a path prefix; external crate names must match whole
    // segments (`serde` but not `serde_json`).
    path == forbidden || path.starts_with(&format!("{forbidden}::"))
}

#[test]
fn inner_layers_do_not_depend_on_outer_layers() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    for (layer, forbidden) in RULES {
        let mut files = Vec::new();
        rust_files(&src.join(layer), &mut files);
        assert!(!files.is_empty(), "no sources found for layer `{layer}`");
        for file in files {
            for path in used_paths(&fs::read_to_string(&file).unwrap()) {
                for f in *forbidden {
                    assert!(
                        !violates(&path, f),
                        "dependency rule violated: {} (layer `{layer}`) uses `{path}`",
                        file.display()
                    );
                }
            }
        }
    }
}

#[test]
fn parser_reads_use_declarations() {
    let source = "use std::fmt;\npub use a::{\n    B,\n    C,\n};\nfn f() {}\n\
                  #[cfg(test)]\nmod t { use tokio::x; }\n";
    assert_eq!(used_paths(source), ["std::fmt", "a::{B,C,}"]);
}
