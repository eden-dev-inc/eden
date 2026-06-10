fn main() {
    println!("cargo::rustc-check-cfg=cfg(external_db)");
}
