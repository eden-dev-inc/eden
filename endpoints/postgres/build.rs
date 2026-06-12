fn main() {
    println!("cargo::rustc-check-cfg=cfg(external_db)");

    if std::env::var_os("CARGO_FEATURE_EMBEDDED_DB").is_none() {
        println!("cargo::rustc-cfg=external_db");
    }
}
