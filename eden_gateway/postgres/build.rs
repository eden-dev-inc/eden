fn main() {
    println!("cargo::rustc-check-cfg=cfg(embedded_db)");

    // `embedded-db` selects the local Turso/in-memory backend.
    if std::env::var_os("CARGO_FEATURE_EMBEDDED_DB").is_some() {
        println!("cargo::rustc-cfg=embedded_db");
    }
}
