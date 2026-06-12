//! Serves the embedded Leptos dashboard build.
//!
//! The dashboard is a client-side WASM SPA built by Trunk into
//! `eden_dashboard/dist`. Those assets are baked into the binary at compile
//! time via [`rust_embed`] and served same-origin alongside the `/api/v1` API,
//! so the browser talks to Eden directly with no separate web server or proxy.

use actix_web::{HttpRequest, HttpResponse, HttpResponseBuilder, http::header};
use dashmap::DashMap;
use flate2::{Compression, write::GzEncoder};
use once_cell::sync::Lazy;
use std::io::Write;

use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../eden_dashboard/dist"]
struct DashboardAssets;

const CONTENT_SECURITY_POLICY: &str = concat!(
    "default-src 'self'; ",
    "script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval'; ",
    "style-src 'self' 'unsafe-inline'; ",
    "img-src 'self' data:; ",
    "font-src 'self' data:; ",
    "connect-src 'self'; ",
    "object-src 'none'; ",
    "base-uri 'self'; ",
    "frame-ancestors 'none'; ",
    "form-action 'self'"
);

const SECURITY_HEADERS: &[(&str, &str)] = &[
    ("Content-Security-Policy", CONTENT_SECURITY_POLICY),
    ("X-Content-Type-Options", "nosniff"),
    ("X-Frame-Options", "DENY"),
    ("Referrer-Policy", "no-referrer"),
    ("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()"),
    ("Cross-Origin-Opener-Policy", "same-origin"),
    ("Cross-Origin-Resource-Policy", "same-origin"),
];

static COMPRESSED_ASSETS: Lazy<DashMap<String, Vec<u8>>> = Lazy::new(DashMap::new);

pub fn has_dashboard_shell() -> bool {
    DashboardAssets::get("index.html").is_some()
}

/// Default-service handler: serve a matching embedded asset, otherwise fall
/// back to the SPA shell (`index.html`) so client-side routes deep-link.
pub async fn serve(req: HttpRequest) -> HttpResponse {
    let path = req.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match DashboardAssets::get(path) {
        Some(asset) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            let mut res = HttpResponse::Ok();
            insert_dashboard_headers(&mut res);
            res.content_type(mime.as_ref());
            // Trunk fingerprints assets (`name-<hash>.ext`), so hashed files are
            // safe to cache forever; the SPA shell must always be revalidated.
            if is_fingerprinted(path) {
                res.insert_header(("Cache-Control", "public, max-age=31536000, immutable"));
            } else {
                res.insert_header(("Cache-Control", "no-cache"));
            }
            asset_body(&req, path, &mut res, asset.data.as_ref())
        }
        None if is_api_path(path) => crate::api_not_found(req).await,
        None if looks_like_static_asset(path) => {
            let mut res = HttpResponse::NotFound();
            insert_dashboard_headers(&mut res);
            res.body("dashboard asset not found")
        }
        None => match DashboardAssets::get("index.html") {
            Some(shell) => {
                let mut res = HttpResponse::Ok();
                insert_dashboard_headers(&mut res);
                res.content_type("text/html; charset=utf-8").insert_header(("Cache-Control", "no-cache"));
                asset_body(&req, "index.html", &mut res, shell.data.as_ref())
            }
            None => {
                let mut res = HttpResponse::NotFound();
                insert_dashboard_headers(&mut res);
                res.body("dashboard build not embedded")
            }
        },
    }
}

fn insert_dashboard_headers(res: &mut HttpResponseBuilder) {
    for &(name, value) in SECURITY_HEADERS {
        res.insert_header((name, value));
    }
}

fn asset_body(req: &HttpRequest, path: &str, res: &mut HttpResponseBuilder, body: &[u8]) -> HttpResponse {
    if let Some((encoding, compressed)) = compress_asset(&req, path, body) {
        res.insert_header((header::CONTENT_ENCODING, encoding));
        res.insert_header((header::VARY, "Accept-Encoding"));
        return res.body(compressed);
    }
    res.body(body.to_vec())
}

fn compress_asset(req: &HttpRequest, path: &str, body: &[u8]) -> Option<(&'static str, Vec<u8>)> {
    if body.len() < 1024 || !is_compressible(path) {
        return None;
    }

    let accept_encoding = req.headers().get(header::ACCEPT_ENCODING)?.to_str().ok()?;
    if accepts_encoding(accept_encoding, "br") {
        return cached_compressed_body(path, "br", body, brotli_compress).map(|compressed| ("br", compressed));
    }
    if accepts_encoding(accept_encoding, "gzip") {
        return cached_compressed_body(path, "gzip", body, gzip_compress).map(|compressed| ("gzip", compressed));
    }
    None
}

fn accepts_encoding(header_value: &str, encoding: &str) -> bool {
    header_value.split(',').any(|part| {
        let mut parts = part.trim().split(';');
        let Some(value) = parts.next() else {
            return false;
        };
        if !value.trim().eq_ignore_ascii_case(encoding) {
            return false;
        }

        parts
            .filter_map(|param| param.trim().strip_prefix("q="))
            .next()
            .and_then(|q| q.parse::<f32>().ok())
            .is_none_or(|quality| quality > 0.0)
    })
}

fn cached_compressed_body(path: &str, encoding: &'static str, body: &[u8], compress: fn(&[u8]) -> Option<Vec<u8>>) -> Option<Vec<u8>> {
    let key = format!("{encoding}:{path}");
    if let Some(cached) = COMPRESSED_ASSETS.get(&key) {
        return Some(cached.clone());
    }

    let compressed = compress(body)?;
    if compressed.len() >= body.len() {
        return None;
    }

    COMPRESSED_ASSETS.insert(key, compressed.clone());
    Some(compressed)
}

fn brotli_compress(body: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    {
        let mut writer = brotli::CompressorWriter::new(&mut out, 4096, 5, 22);
        writer.write_all(body).ok()?;
    }
    Some(out)
}

fn gzip_compress(body: &[u8]) -> Option<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(body).ok()?;
    encoder.finish().ok()
}

fn is_compressible(path: &str) -> bool {
    matches!(path.rsplit('.').next(), Some("css" | "html" | "js" | "json" | "svg" | "txt" | "wasm"))
}

/// Trunk emits content-hashed filenames like `eden_dashboard-1a2b3c.js`.
fn is_fingerprinted(path: &str) -> bool {
    matches!(path.rsplit('.').next(), Some("js" | "wasm" | "css")) && path.contains('-')
}

fn is_api_path(path: &str) -> bool {
    path == "api" || path.starts_with("api/") || path == "proxy" || path.starts_with("proxy/")
}

fn looks_like_static_asset(path: &str) -> bool {
    matches!(
        path.rsplit('.').next(),
        Some("css" | "js" | "json" | "map" | "png" | "jpg" | "jpeg" | "svg" | "wasm" | "webp" | "woff" | "woff2")
    )
}
