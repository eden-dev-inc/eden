//! Verifies the embedded-dashboard static handler: SPA shell at `/`, deep-link
//! fallback to `index.html`, and immutable caching for fingerprinted assets.
//! Runs without a database — it only mounts `webui::serve` as the default
//! service. Requires the dashboard to be built (`trunk build` in eden_dashboard)
//! and the `embedded-dashboard` feature.
#![cfg(feature = "embedded-dashboard")]

use actix_web::{App, http::header, test, web};
use eden_service::webui;

fn cache_control(resp: &actix_web::dev::ServiceResponse) -> String {
    resp.headers().get(header::CACHE_CONTROL).and_then(|v| v.to_str().ok()).unwrap_or_default().to_string()
}

fn content_type(resp: &actix_web::dev::ServiceResponse) -> String {
    resp.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or_default().to_string()
}

fn header_value(resp: &actix_web::dev::ServiceResponse, name: &'static str) -> String {
    resp.headers().get(name).and_then(|v| v.to_str().ok()).unwrap_or_default().to_string()
}

fn wasm_asset_name() -> String {
    let dist = concat!(env!("CARGO_MANIFEST_DIR"), "/../eden_dashboard/dist");
    std::fs::read_dir(dist)
        .expect("dashboard dist/ must exist — run `trunk build` in eden_dashboard")
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .find(|n| n.ends_with(".wasm"))
        .expect("a .wasm asset should exist in dist/")
}

#[actix_web::test]
async fn embedded_assets_include_dashboard_shell() {
    assert!(webui::has_dashboard_shell());
}

#[actix_web::test]
async fn root_serves_html_shell_revalidated() {
    let app = test::init_service(App::new().default_service(web::to(webui::serve))).await;

    let resp = test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;

    assert!(resp.status().is_success());
    assert!(content_type(&resp).starts_with("text/html"));
    assert_eq!(cache_control(&resp), "no-cache");
    assert!(header_value(&resp, "Content-Security-Policy").contains("default-src 'self'"));
    assert_eq!(header_value(&resp, "X-Content-Type-Options"), "nosniff");
    assert_eq!(header_value(&resp, "X-Frame-Options"), "DENY");
}

#[actix_web::test]
async fn shell_is_self_contained_without_external_font_hosts() {
    let app = test::init_service(App::new().default_service(web::to(webui::serve))).await;

    let resp = test::call_service(&app, test::TestRequest::get().uri("/").to_request()).await;
    let body = test::read_body(resp).await;
    let html = String::from_utf8_lossy(&body);

    assert!(!html.contains("fonts.googleapis.com"));
    assert!(!html.contains("fonts.gstatic.com"));
}

#[actix_web::test]
async fn unknown_route_falls_back_to_shell() {
    let app = test::init_service(App::new().default_service(web::to(webui::serve))).await;

    let resp = test::call_service(&app, test::TestRequest::get().uri("/dashboard/endpoints").to_request()).await;

    assert!(resp.status().is_success());
    assert!(content_type(&resp).starts_with("text/html"));
}

#[actix_web::test]
async fn api_route_does_not_fall_back_to_shell() {
    let app = test::init_service(App::new().default_service(web::to(webui::serve))).await;

    let resp = test::call_service(&app, test::TestRequest::get().uri("/api/v1/not-real").to_request()).await;

    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
    assert!(content_type(&resp).starts_with("application/json"));
}

#[actix_web::test]
async fn missing_static_asset_does_not_fall_back_to_shell() {
    let app = test::init_service(App::new().default_service(web::to(webui::serve))).await;

    let resp = test::call_service(&app, test::TestRequest::get().uri("/missing-dashboard-asset.js").to_request()).await;

    assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);
}

#[actix_web::test]
async fn fingerprinted_wasm_is_immutable() {
    let wasm = wasm_asset_name();

    let app = test::init_service(App::new().default_service(web::to(webui::serve))).await;

    let uri = format!("/{wasm}");
    let resp = test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

    assert!(resp.status().is_success());
    assert_eq!(content_type(&resp), "application/wasm");
    assert_eq!(cache_control(&resp), "public, max-age=31536000, immutable");
}

#[actix_web::test]
async fn compresses_large_dashboard_assets_when_supported() {
    let wasm = wasm_asset_name();
    let app = test::init_service(App::new().default_service(web::to(webui::serve))).await;

    let uri = format!("/{wasm}");
    let resp = test::call_service(
        &app,
        test::TestRequest::get().uri(&uri).insert_header((header::ACCEPT_ENCODING, "gzip")).to_request(),
    )
    .await;

    assert!(resp.status().is_success());
    assert_eq!(header_value(&resp, "Content-Encoding"), "gzip");
    assert_eq!(header_value(&resp, "Vary"), "Accept-Encoding");
}

#[actix_web::test]
async fn compression_respects_disabled_encoding_quality() {
    let wasm = wasm_asset_name();
    let app = test::init_service(App::new().default_service(web::to(webui::serve))).await;

    let uri = format!("/{wasm}");
    let resp = test::call_service(
        &app,
        test::TestRequest::get().uri(&uri).insert_header((header::ACCEPT_ENCODING, "br;q=0, gzip")).to_request(),
    )
    .await;

    assert!(resp.status().is_success());
    assert_eq!(header_value(&resp, "Content-Encoding"), "gzip");
}
