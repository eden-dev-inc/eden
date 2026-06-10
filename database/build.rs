fn main() {
    println!("cargo::rustc-check-cfg=cfg(embedded_db)");

    // `embedded-db` enables the Turso-backed row/schema types in endpoint-core,
    // so database flips the matching cfg whenever the embedded backend is selected.
    if std::env::var_os("CARGO_FEATURE_EMBEDDED_DB").is_some() {
        println!("cargo::rustc-cfg=embedded_db");
    }
}
