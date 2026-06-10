use std::env;

use eden_core::error::{CommonError as Error, ResultCommon as Result};

const PREFIX: &str = "EDEN_LB_";
const PORT: &str = "PORT";
const PORT_DEFAULT: &str = "7999";
const HTTPS_PORT: &str = "HTTPS_PORT";
const HTTPS_PORT_DEFAULT: &str = "7998";
//TODO make sure these ENV are changed to "Eden" from "Relay"
const EDEN: &str = "RELAY";
const EDEN_DEFAULT: &str = "localhost:8000";
const EDEN_SNI: &str = "EDEN_SNI";
const EDEN_SNI_DEFAULT: &str = "eden.edenlabs.cloud";
const OTLP_COLLECTOR: &str = "OTLP_COLLECTOR";
const OTLP_COLLECTOR_DEFAULT: &str = "http://localhost:4317";

#[derive(Clone)]
pub struct Config {
    http_port: u16,
    https_port: u16,
    eden_url: String,
    eden_sni: String,
    otlp_collector: String,
}

impl Config {
    pub fn new() -> Result<Self> {
        let port_str = env::var(PREFIX.to_string() + PORT).unwrap_or(PORT_DEFAULT.to_string());
        let https_port_str = env::var(PREFIX.to_string() + HTTPS_PORT).unwrap_or(HTTPS_PORT_DEFAULT.to_string());
        let relay_url = env::var(PREFIX.to_string() + EDEN).unwrap_or(EDEN_DEFAULT.to_string());
        let relay_sni = env::var(PREFIX.to_string() + EDEN_SNI).unwrap_or(EDEN_SNI_DEFAULT.to_string());
        let port = match str::parse::<u16>(&port_str) {
            Ok(p) => {
                if p == 0 {
                    return Err(Error::Config(format!("invalid {}{}=0", PREFIX, PORT)));
                } else {
                    p
                }
            }
            Err(e) => {
                return Err(Error::Config(format!("invalid {}{}={}: {}", PREFIX, PORT, port_str, e)));
            }
        };
        let https_port = match str::parse::<u16>(&https_port_str) {
            Ok(p) => {
                if p == 0 {
                    return Err(Error::Config(format!("invalid {}{}=0", PREFIX, HTTPS_PORT)));
                } else {
                    p
                }
            }
            Err(e) => {
                return Err(Error::Config(format!("invalid {}{}={}: {}", PREFIX, HTTPS_PORT, https_port_str, e)));
            }
        };
        let otlp_collector = env::var(OTLP_COLLECTOR).unwrap_or(OTLP_COLLECTOR_DEFAULT.to_string());

        Ok(Self {
            http_port: port,
            https_port,
            eden_url: relay_url,
            eden_sni: relay_sni,
            otlp_collector,
        })
    }

    pub fn port(&self) -> u16 {
        self.http_port
    }

    pub fn https_port(&self) -> u16 {
        self.https_port
    }

    pub fn eden_url(&self) -> &str {
        &self.eden_url
    }

    pub fn eden_sni(&self) -> &str {
        &self.eden_sni
    }

    pub fn otlp_collector(&self) -> &str {
        &self.otlp_collector
    }
}
