//! RESP framing and client-visible response helpers.

use super::*;

pub(crate) struct RedisWire;

impl RedisWire {
    #[inline]
    pub(crate) fn is_resp_null(data: &[u8]) -> bool {
        data == b"$-1\r\n" || data == b"*-1\r\n"
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn should_fallback_on_miss(result: &ResultEP<Bytes>) -> bool {
        match result {
            Err(_) => true,
            Ok(resp) => Self::is_resp_null(resp),
        }
    }

    #[inline]
    pub(crate) fn response_contains_redis_error(resp: &[u8]) -> bool {
        resp.starts_with(b"-") || resp.windows(3).any(|w| w == b"\r\n-")
    }

    #[inline]
    pub(crate) fn format_resp_error_line(message: &str) -> Bytes {
        let sanitized = message.replace(['\r', '\n'], " ");
        Bytes::from(format!("-ERR {}\r\n", sanitized))
    }

    #[inline]
    pub(crate) fn count_resp_line_terminators(frame: &[u8]) -> usize {
        frame.windows(2).filter(|window| *window == b"\r\n").count()
    }

    #[inline]
    pub(crate) fn client_visible_response_slots(result: &ResultEP<Option<Bytes>>) -> usize {
        match result {
            Ok(Some(_)) | Ok(None) | Err(_) => 1,
        }
    }

    pub(crate) fn render_client_response_bytes(results: &[ResultEP<Option<Bytes>>]) -> Bytes {
        let mut response_buffer = BytesMut::new();
        for result in results {
            match result {
                Ok(Some(resp)) => response_buffer.extend_from_slice(resp),
                Ok(None) => response_buffer.extend_from_slice(&Self::format_resp_error_line(MISSING_UPSTREAM_RESPONSE_MESSAGE)),
                Err(err) => response_buffer.extend_from_slice(&Self::format_resp_error_line(&err.to_string())),
            }
        }
        response_buffer.freeze()
    }

    #[inline]
    pub(crate) fn normalize_result_for_client(result: ResultEP<Option<Bytes>>) -> ResultEP<Option<Bytes>> {
        match result {
            Ok(Some(resp)) => Ok(Some(resp)),
            Ok(None) => Err(EpError::parse(MISSING_UPSTREAM_RESPONSE_MESSAGE)),
            Err(err) => Err(err),
        }
    }

    #[inline]
    pub(crate) fn append_bounded(buffer: &mut BytesMut, payload: &[u8], limit: usize) -> Result<(), EpError> {
        if buffer.len().saturating_add(payload.len()) > limit {
            Err(EpError::request(format!("buffer limit exceeded ({limit} bytes)")))
        } else {
            buffer.extend_from_slice(payload);
            Ok(())
        }
    }

    #[inline]
    pub(crate) fn session_state_rejection(command: &RedisApi) -> Option<Bytes> {
        let message = match command {
            RedisApi::Subscribe
            | RedisApi::Psubscribe
            | RedisApi::Ssubscribe
            | RedisApi::Unsubscribe
            | RedisApi::Punsubscribe
            | RedisApi::Sunsubscribe => UNSUPPORTED_PUBSUB_MESSAGE,
            RedisApi::Auth => UNSUPPORTED_AUTH_MESSAGE,
            RedisApi::Select => UNSUPPORTED_SELECT_MESSAGE,
            _ => return None,
        };
        Some(Self::format_resp_error_line(message))
    }

    pub(crate) fn measure_request_buffer_retention(chunks: &[Bytes]) -> (usize, usize) {
        let mut buffer = BytesMut::with_capacity(16 * 1024);
        let mut parsed_commands = 0;

        for chunk in chunks {
            buffer.extend_from_slice(chunk);
            while let Ok(Some((_parsed, consumed))) = endpoints::endpoint::ep_redis::protocol::RedisProtocol::parse_buffer(&buffer) {
                let _ = buffer.split_to(consumed);
                parsed_commands += 1;
            }
        }

        (buffer.len(), parsed_commands)
    }

    #[cfg(test)]
    pub(crate) fn request_buffer_hits_limit(chunks: &[Bytes]) -> bool {
        let mut buffer = BytesMut::with_capacity(16 * 1024);
        for chunk in chunks {
            if Self::append_bounded(&mut buffer, chunk, MAX_REQUEST_BUFFER_BYTES).is_err() {
                return true;
            }
        }
        false
    }
}
