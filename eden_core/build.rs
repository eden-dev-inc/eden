fn main() {
    // Generated gRPC bindings are checked in under `eden_core/proto/src/proto.rs`.
    // Normal builds should not regenerate them, but keep the source proto visible
    // to Cargo so proto edits still invalidate this crate's build state.
    println!("cargo:rerun-if-changed=proto/proto.proto");
}
