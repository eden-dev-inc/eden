fn main() {
    println!("cargo::rustc-check-cfg=cfg(embedded_db)");
    println!("cargo::rustc-check-cfg=cfg(external_db)");

    // `embedded-db` selects the local Turso/in-memory backend. When the embedded
    // backend is not selected we build against external Redis/Postgres.
    if std::env::var_os("CARGO_FEATURE_EMBEDDED_DB").is_some() {
        println!("cargo::rustc-cfg=embedded_db");
    } else {
        println!("cargo::rustc-cfg=external_db");
    }

    // The `embedded-dashboard` feature embeds `../eden_dashboard/dist` via
    // `#[derive(RustEmbed)]` (see src/webui.rs). That derive fails to COMPILE if
    // the folder is absent — which it is in any build that didn't run Trunk
    // first (CI doctests/clippy under `--all-features`, fresh checkouts). `dist`
    // is a git-ignored build artifact, so guarantee the directory exists here.
    // An empty dir compiles fine; `webui::serve` already 404s when no asset is
    // embedded, and a real Trunk build overwrites it with the actual bundle.
    if std::env::var_os("CARGO_FEATURE_EMBEDDED_DASHBOARD").is_some() {
        let dist = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../eden_dashboard/dist");
        println!("cargo::rerun-if-changed={}", dist.join("index.html").display());
        if let Err(e) = std::fs::create_dir_all(&dist) {
            println!("cargo::warning=could not ensure {} exists: {e}", dist.display());
        }
        if std::env::var_os("EDEN_REQUIRE_EMBEDDED_DASHBOARD_ASSETS").is_some() && !dist.join("index.html").is_file() {
            panic!(
                "embedded-dashboard build requires {}; run `trunk build --release` in eden_dashboard first",
                dist.join("index.html").display()
            );
        }
    }
}
