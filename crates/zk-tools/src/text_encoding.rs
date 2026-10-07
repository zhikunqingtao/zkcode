//! Strict text decoding and reversible writes. A Latin-1 fallback is a preview,
//! never proof that arbitrary bytes are text or permission to rewrite them.

/// Supported explicit file encodings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TextEncoding {
    /// UTF-8, with any BOM tracked separately.
    #[default]
    Utf8,
    /// Little-endian UTF-16.
    Utf16Le,
    /// Big-endian UTF-16.
    Utf16Be,
    /// ISO-8859-1; distinct from Windows-1252.
    Latin1,
    /// GB18030 (explicit selection only).
    Gb18030,
}

/// Encoding identity observed in a complete physical read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TextFormat {
    /// Character encoding.
    pub encoding: TextEncoding,
    /// Whether the original bytes started with the encoding's BOM.
    pub bom: bool,
}

/// A decoded view and whether it can safely authorize byte-preserving editing.
pub struct DecodedText {
    /// Text with the transport BOM removed; original newlines remain untouched.
    pub text: String,
    /// Actual encoding and BOM policy.
    pub format: TextFormat,
    /// A guessed Latin-1 view must be explicitly confirmed by another Read.
    pub fallback: bool,
    /// Exact decode/encode round trip, including BOM and noncanonical mappings.
    pub reversible: bool,
}

impl TextEncoding {
    /// Canonical diagnostic label; never implies automatic GB18030 detection.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Utf8 => "UTF-8",
            Self::Utf16Le => "UTF-16LE",
            Self::Utf16Be => "UTF-16BE",
            Self::Latin1 => "ISO-8859-1",
            Self::Gb18030 => "GB18030",
        }
    }
    fn parse(value: &str) -> Result<Self, &'static str> {
        match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "utf-8" | "utf8" => Ok(Self::Utf8),
            "utf-16le" | "utf16le" => Ok(Self::Utf16Le),
            "utf-16be" | "utf16be" => Ok(Self::Utf16Be),
            "iso-8859-1" | "latin1" | "latin-1" => Ok(Self::Latin1),
            "gb18030" => Ok(Self::Gb18030),
            _ => Err("FILE_ENCODING_UNSUPPORTED"),
        }
    }
}

impl TextFormat {
    /// Encode without substitution or implicit newline normalization.
    ///
    /// # Errors
    /// Characters that cannot be represented return a stable error before writing.
    pub fn encode(self, text: &str) -> Result<Vec<u8>, &'static str> {
        let mut bytes = Vec::new();
        if self.bom {
            bytes.extend_from_slice(match self.encoding {
                TextEncoding::Utf8 => &[0xef, 0xbb, 0xbf],
                TextEncoding::Utf16Le => &[0xff, 0xfe],
                TextEncoding::Utf16Be => &[0xfe, 0xff],
                _ => return Err("FILE_ENCODING_BOM_MISMATCH"),
            });
        }
        match self.encoding {
            TextEncoding::Utf8 => bytes.extend_from_slice(text.as_bytes()),
            TextEncoding::Utf16Le | TextEncoding::Utf16Be => {
                for unit in text.encode_utf16() {
                    let pair = if self.encoding == TextEncoding::Utf16Le {
                        unit.to_le_bytes()
                    } else {
                        unit.to_be_bytes()
                    };
                    bytes.extend_from_slice(&pair);
                }
            }
            TextEncoding::Latin1 => {
                for ch in text.chars() {
                    bytes.push(
                        u8::try_from(u32::from(ch)).map_err(|_| "FILE_ENCODING_UNREPRESENTABLE")?,
                    );
                }
            }
            TextEncoding::Gb18030 => {
                let (encoded, _, errors) = encoding_rs::GB18030.encode(text);
                if errors {
                    return Err("FILE_ENCODING_UNREPRESENTABLE");
                }
                bytes.extend_from_slice(&encoded);
            }
        }
        Ok(bytes)
    }
}

/// Decode an observed file. Explicit selections never silently switch encodings.
///
/// # Errors
/// Rejects binary/control-heavy input, malformed selected encodings and BOM conflicts.
pub fn decode(bytes: &[u8], explicit: Option<&str>) -> Result<DecodedText, &'static str> {
    let explicit = explicit.map(TextEncoding::parse).transpose()?;
    let bom = if bytes.starts_with(&[0xef, 0xbb, 0xbf]) {
        Some((TextEncoding::Utf8, 3))
    } else if bytes.starts_with(&[0xff, 0xfe]) {
        Some((TextEncoding::Utf16Le, 2))
    } else if bytes.starts_with(&[0xfe, 0xff]) {
        Some((TextEncoding::Utf16Be, 2))
    } else {
        None
    };
    if let (Some(selected), Some((detected, _))) = (explicit, bom)
        && selected != detected
    {
        return Err("FILE_ENCODING_BOM_MISMATCH");
    }
    let fallback = explicit.is_none() && bom.is_none() && std::str::from_utf8(bytes).is_err();
    let encoding = explicit
        .or(bom.map(|(encoding, _)| encoding))
        .unwrap_or(if fallback {
            TextEncoding::Latin1
        } else {
            TextEncoding::Utf8
        });
    let payload = &bytes[bom.map_or(0, |(_, size)| size)..];
    if !matches!(encoding, TextEncoding::Utf16Le | TextEncoding::Utf16Be) && looks_binary(payload) {
        return Err("FILE_BINARY_UNSUPPORTED");
    }
    let text = match encoding {
        TextEncoding::Utf8 => std::str::from_utf8(payload)
            .map_err(|_| "FILE_ENCODING_INVALID")?
            .to_owned(),
        TextEncoding::Latin1 => payload.iter().map(|byte| char::from(*byte)).collect(),
        TextEncoding::Gb18030 => encoding_rs::GB18030
            .decode_without_bom_handling_and_without_replacement(payload)
            .ok_or("FILE_ENCODING_INVALID")?
            .into_owned(),
        TextEncoding::Utf16Le | TextEncoding::Utf16Be => {
            if !payload.len().is_multiple_of(2) {
                return Err("FILE_ENCODING_INVALID");
            }
            let units = payload.chunks_exact(2).map(|p| {
                if encoding == TextEncoding::Utf16Le {
                    u16::from_le_bytes([p[0], p[1]])
                } else {
                    u16::from_be_bytes([p[0], p[1]])
                }
            });
            char::decode_utf16(units)
                .collect::<Result<String, _>>()
                .map_err(|_| "FILE_ENCODING_INVALID")?
        }
    };
    if looks_binary(text.as_bytes()) {
        return Err("FILE_BINARY_UNSUPPORTED");
    }
    let format = TextFormat {
        encoding,
        bom: bom.is_some(),
    };
    let reversible = format.encode(&text).is_ok_and(|encoded| encoded == bytes);
    Ok(DecodedText {
        text,
        format,
        fallback,
        reversible,
    })
}

fn looks_binary(bytes: &[u8]) -> bool {
    if [b"\x7fELF".as_slice(), b"PK\x03\x04", b"\x1f\x8b", b"%PDF-"]
        .iter()
        .any(|magic| bytes.starts_with(magic))
    {
        return true;
    }
    let sample = &bytes[..bytes.len().min(8192)];
    sample.contains(&0)
        || sample
            .iter()
            .filter(|b| **b < 8 || (**b > 13 && **b < 32 && **b != 27))
            .count()
            * 20
            > sample.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boms_and_mixed_newlines_roundtrip_without_normalization() {
        for encoding in [
            TextEncoding::Utf8,
            TextEncoding::Utf16Le,
            TextEncoding::Utf16Be,
        ] {
            let format = TextFormat {
                encoding,
                bom: true,
            };
            let text = "中文𠮷\r\nline\rlast\n";
            let bytes = format.encode(text).unwrap();
            let decoded = decode(&bytes, None).unwrap();
            assert_eq!(decoded.text, text);
            assert_eq!(decoded.format, format);
            assert!(decoded.reversible && !decoded.fallback);
            assert!(decode(&bytes, Some("latin1")).is_err());
        }
    }

    #[test]
    fn explicit_gb18030_and_latin1_never_use_replacement_characters() {
        let format = TextFormat {
            encoding: TextEncoding::Gb18030,
            bom: false,
        };
        let bytes = format.encode("中文😀\r\n").unwrap();
        let decoded = decode(&bytes, Some("GB18030")).unwrap();
        assert_eq!(decoded.text, "中文😀\r\n");
        assert!(decoded.reversible && !decoded.fallback);
        assert!(decode(&[0x81], Some("GB18030")).is_err());
        let preview = decode(b"caf\xe9\r\n", None).unwrap();
        assert_eq!(preview.text, "café\r\n");
        assert!(preview.fallback);
        assert!(!decode(b"caf\xe9\r\n", Some("latin1")).unwrap().fallback);
        assert!(preview.format.encode("中文").is_err());
    }

    #[test]
    fn malformed_unicode_and_binary_are_rejected_even_with_explicit_latin1() {
        for bytes in [
            &[0xff, 0xfe, 0x00, 0xd8][..],
            &[0xfe, 0xff, 0x00][..],
            &[0xef, 0xbb, 0xbf, 0xff][..],
        ] {
            assert!(decode(bytes, None).is_err());
        }
        for bytes in [
            b"a\0b".as_slice(),
            b"PK\x03\x04payload",
            b"\x01\x02\x03\x04",
        ] {
            assert!(decode(bytes, Some("latin1")).is_err());
        }
        assert!(decode(b"\xff", Some("utf8")).is_err());
        assert!(decode(b"valid", Some("unknown")).is_err());
    }
}
