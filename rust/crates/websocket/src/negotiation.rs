//! RFC 6455 extension grammar and RFC 7692 server negotiation.
use axum::http::{header::SEC_WEBSOCKET_EXTENSIONS, HeaderMap, HeaderValue};

#[derive(Debug)]
pub(crate) struct Agreement {
    pub(crate) server_window: u8,
    pub(crate) client_window: u8,
    pub(crate) response: HeaderValue,
}

// Invalid HTTP extension syntax rejects the handshake. Invalid/unsupported PMD
// parameters decline only that offer, allowing the client's next fallback.
pub(crate) fn negotiate(headers: &HeaderMap) -> Result<Option<Agreement>, ()> {
    let mut selected = None;
    for value in headers.get_all(SEC_WEBSOCKET_EXTENSIONS) {
        let mut parser = Parser(value.to_str().map_err(|_| ())?.as_bytes());
        loop {
            parser.ows();
            if parser.0.is_empty() {
                return Err(());
            }
            let name = parser.token()?;
            let mut valid = true;
            let mut seen = 0u8;
            let mut server_window = 12;
            let mut client_window = 15;
            let mut client_window_offered = false;
            loop {
                parser.ows();
                if !parser.take(b';') {
                    break;
                }
                parser.ows();
                let parameter = parser.token()?;
                parser.ows();
                let value = if parser.take(b'=') {
                    parser.ows();
                    Some(parser.value()?)
                } else {
                    None
                };
                if name != "permessage-deflate" {
                    continue;
                }
                let bit = match parameter {
                    "server_no_context_takeover" => 1,
                    "client_no_context_takeover" => 2,
                    "server_max_window_bits" => 4,
                    "client_max_window_bits" => 8,
                    _ => {
                        valid = false;
                        continue;
                    }
                };
                if seen & bit != 0 {
                    valid = false;
                }
                seen |= bit;
                match bit {
                    1 | 2 => valid &= value.is_none(),
                    4 => match value.as_deref().and_then(window) {
                        // flate2's compressor supports 9..=15, never promise 8.
                        Some(bits @ 9..=15) => server_window = bits.min(12),
                        _ => valid = false,
                    },
                    8 => {
                        client_window_offered = true;
                        if let Some(value) = value {
                            match window(&value) {
                                Some(bits) => client_window = bits,
                                None => valid = false,
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            }
            if name == "permessage-deflate" && valid && selected.is_none() {
                let mut response = format!("permessage-deflate; server_no_context_takeover; client_no_context_takeover; server_max_window_bits={server_window}");
                if client_window_offered {
                    response.push_str(&format!("; client_max_window_bits={client_window}"));
                }
                selected = Some(Agreement {
                    server_window,
                    client_window,
                    response: response.parse().map_err(|_| ())?,
                });
            }
            parser.ows();
            if parser.0.is_empty() {
                break;
            }
            if !parser.take(b',') {
                return Err(());
            }
        }
    }
    Ok(selected)
}

fn window(value: &str) -> Option<u8> {
    match value {
        "8" => Some(8),
        "9" => Some(9),
        "10" => Some(10),
        "11" => Some(11),
        "12" => Some(12),
        "13" => Some(13),
        "14" => Some(14),
        "15" => Some(15),
        _ => None,
    }
}

fn token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

struct Parser<'a>(&'a [u8]);
impl<'a> Parser<'a> {
    fn ows(&mut self) {
        while self.0.first().is_some_and(|b| matches!(b, b' ' | b'\t')) {
            self.0 = &self.0[1..];
        }
    }
    fn take(&mut self, byte: u8) -> bool {
        if self.0.first() == Some(&byte) {
            self.0 = &self.0[1..];
            true
        } else {
            false
        }
    }
    fn token(&mut self) -> Result<&'a str, ()> {
        let end = self
            .0
            .iter()
            .position(|b| !token_byte(*b))
            .unwrap_or(self.0.len());
        if end == 0 {
            return Err(());
        }
        let token = std::str::from_utf8(&self.0[..end]).map_err(|_| ())?;
        self.0 = &self.0[end..];
        Ok(token)
    }
    fn value(&mut self) -> Result<String, ()> {
        if !self.take(b'"') {
            return self.token().map(str::to_owned);
        }
        let mut value = String::new();
        loop {
            let Some((&byte, rest)) = self.0.split_first() else {
                return Err(());
            };
            self.0 = rest;
            if byte == b'"' {
                break;
            }
            let byte = if byte == b'\\' {
                let Some((&escaped, rest)) = self.0.split_first() else {
                    return Err(());
                };
                self.0 = rest;
                escaped
            } else {
                byte
            };
            // RFC 6455 requires the unescaped quoted value to be a token.
            if !token_byte(byte) {
                return Err(());
            }
            value.push(byte as char);
        }
        if value.is_empty() {
            return Err(());
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn select(value: &str) -> Result<Option<Agreement>, ()> {
        let mut headers = HeaderMap::new();
        headers.insert(SEC_WEBSOCKET_EXTENSIONS, value.parse().unwrap());
        negotiate(&headers)
    }
    #[test]
    fn settings_and_windows() {
        for bits in 8..=15 {
            let selected = select(&format!(
                "permessage-deflate; server_max_window_bits={bits}; client_max_window_bits={bits}"
            ))
            .unwrap();
            if bits == 8 {
                assert!(selected.is_none());
                continue;
            }
            let selected = selected.unwrap();
            assert_eq!(selected.server_window, bits.min(12));
            assert_eq!(selected.client_window, bits);
        }
        let selected = select("permessage-deflate; client_max_window_bits=8")
            .unwrap()
            .unwrap();
        assert_eq!(selected.client_window, 8);
        for offer in [
            "permessage-deflate",
            "permessage-deflate; server_no_context_takeover; client_no_context_takeover",
            "permessage-deflate; client_max_window_bits",
            "permessage-deflate; server_max_window_bits=\"1\\2\"; client_max_window_bits = \"9\"",
        ] {
            let selected = select(offer).unwrap().unwrap();
            let response = selected.response.to_str().unwrap();
            assert!(response.contains("server_no_context_takeover; client_no_context_takeover"));
            assert_eq!(
                response.contains("client_max_window_bits="),
                offer.contains("client_max_window_bits")
            );
        }
    }
    #[test]
    fn invalid_parameters_decline_only_the_offer() {
        for parameter in [
            "unknown",
            "server_no_context_takeover=1",
            "client_no_context_takeover=0",
            "server_max_window_bits",
            "server_max_window_bits=8",
            "server_max_window_bits=07",
            "client_max_window_bits=16",
            "client_max_window_bits=-9",
            "client_max_window_bits=99999999999999999999",
            "server_max_window_bits=12; server_max_window_bits=12",
            "client_max_window_bits; client_max_window_bits=9",
            "server_no_context_takeover; server_no_context_takeover",
            "client_no_context_takeover; client_no_context_takeover",
        ] {
            let bad = format!("permessage-deflate; {parameter}");
            assert!(select(&bad).unwrap().is_none(), "{bad}");
            let fallback = select(&format!(
                "{bad}, permessage-deflate; server_max_window_bits=10"
            ))
            .unwrap()
            .unwrap();
            assert_eq!(fallback.server_window, 10);
        }
    }
    #[test]
    fn grammar_multiple_headers_and_unknown_extensions() {
        let mut headers = HeaderMap::new();
        headers.append(
            SEC_WEBSOCKET_EXTENSIONS,
            "unknown; foo=\"bar\", permessage-deflate; server_max_window_bits=8"
                .parse()
                .unwrap(),
        );
        headers.append(
            SEC_WEBSOCKET_EXTENSIONS,
            "\tpermessage-deflate ; server_max_window_bits = 9\t"
                .parse()
                .unwrap(),
        );
        assert_eq!(negotiate(&headers).unwrap().unwrap().server_window, 9);
        assert!(select("unknown; p=token").unwrap().is_none());
        for bad in [
            "",
            ",",
            "permessage-deflate,",
            ",permessage-deflate",
            "permessage-deflate,,other",
            "permessage-deflate;",
            "permessage-deflate; =12",
            "permessage-deflate; x=",
            "permessage-deflate; x=\"\"",
            "permessage-deflate; x=\"12",
            "permessage-deflate; x=\"1,2\"",
            "permessage-deflate; x=12=13",
            "permessage-deflate; x=\"12\"garbage",
            "permessage-deflate, other; x=\"\\\"",
        ] {
            assert!(select(bad).is_err(), "{bad}");
        }
    }
    #[test]
    fn arbitrary_headers_never_panic_or_negotiate_unsafe_settings() {
        let alphabet = b"permessage-deflate;client_max_window_bits=89,15\"\\ \t!\x7f";
        let mut seed = 123u32;
        for _ in 0..10000 {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            let size = seed as usize % 256;
            let mut bytes = Vec::with_capacity(size);
            for _ in 0..size {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                bytes.push(alphabet[seed as usize % alphabet.len()]);
            }
            if let Ok(value) = HeaderValue::from_bytes(&bytes) {
                let mut headers = HeaderMap::new();
                headers.insert(SEC_WEBSOCKET_EXTENSIONS, value);
                if let Ok(Some(selected)) = negotiate(&headers) {
                    assert!((9..=12).contains(&selected.server_window));
                    assert!((8..=15).contains(&selected.client_window));
                    assert!(selected
                        .response
                        .to_str()
                        .unwrap()
                        .contains("client_no_context_takeover"));
                }
            }
        }
    }
}
