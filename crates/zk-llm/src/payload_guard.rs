//! Final wire limits and scoped media estimates, independent of provider billing.
use crate::{ProviderError, capabilities_for, is_known_model};
use base64::Engine as _;
use serde_json::Value;
use std::io::Write;

const MAX_WIRE: usize = 64 * 1024 * 1024;
const MAX_IMAGE: usize = 10 * 1024 * 1024;
const MAX_PIXELS: u64 = 40_000_000;
const PNG_SIGNATURE: &[u8] = b"\x89PNG\r\n\x1a\n";

/// Conservative header-only raster admission estimate; unknown and animated
/// formats retain byte accounting. This is not reported provider usage or proof
/// that every compressed pixel is decodable. No raster or metadata is inflated.
/// # Errors
/// Oversized encodings and raster dimensions fail before a network call.
pub fn inline_image_tokens(encoded: &str) -> Result<u64, ProviderError> {
    if encoded.len() > MAX_IMAGE.div_ceil(3) * 4 {
        return Err(invalid("IMAGE_SIZE_EXCEEDED"));
    }
    let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return Ok(encoded.len() as u64);
    };
    if bytes.len() > MAX_IMAGE {
        return Err(invalid("IMAGE_SIZE_EXCEEDED"));
    }
    let Some((width, height, animated)) = dimensions(&bytes) else {
        return Ok(encoded.len() as u64);
    };
    if width == 0 || height == 0 || u64::from(width) * u64::from(height) > MAX_PIXELS {
        return Err(invalid("IMAGE_PIXEL_LIMIT_EXCEEDED"));
    }
    if animated {
        return Ok(encoded.len() as u64);
    }
    Ok(1024 + u64::from(width).div_ceil(16) * u64::from(height).div_ceil(16))
}

/// Header-only validation for image transcoding; does not inflate raster/metadata.
/// # Errors
/// Rejects unknown/truncated headers, zero/oversized dimensions and payloads over 10 MiB.
pub fn image_media_type(bytes: &[u8]) -> Result<&'static str, ProviderError> {
    if bytes.len() > MAX_IMAGE {
        return Err(invalid("IMAGE_SIZE_EXCEEDED"));
    }
    let Some((width, height, _)) = dimensions(bytes) else {
        return Err(invalid("IMAGE_CONTENT_TYPE_INVALID"));
    };
    if width == 0 || height == 0 || u64::from(width) * u64::from(height) > MAX_PIXELS {
        return Err(invalid("IMAGE_PIXEL_LIMIT_EXCEEDED"));
    }
    Ok(if bytes.starts_with(PNG_SIGNATURE) {
        "image/png"
    } else if bytes.starts_with(b"GIF8") {
        "image/gif"
    } else if bytes.starts_with(b"RIFF") {
        "image/webp"
    } else {
        "image/jpeg"
    })
}

/// Dimensions from the same bounded header parser used for final wire admission.
/// No raster pixels or optional metadata are decompressed.
/// # Errors
/// Rejects unsupported headers, zero/oversized dimensions and payloads over 10 MiB.
pub fn validated_image_dimensions(bytes: &[u8]) -> Result<(u32, u32), ProviderError> {
    image_media_type(bytes)?;
    dimensions(bytes)
        .map(|(width, height, _)| (width, height))
        .ok_or_else(|| invalid("IMAGE_CONTENT_TYPE_INVALID"))
}

/// Require bounded raster headers and complete outer containers without decoding pixels.
/// # Errors
/// Rejects missing trailers, truncated chunks/subblocks and header-only fake images.
pub fn complete_image_media_type(bytes: &[u8]) -> Result<&'static str, ProviderError> {
    let media = image_media_type(bytes)?;
    let complete = match media {
        "image/png" => complete_png(bytes).is_some(),
        "image/jpeg" => complete_jpeg(bytes).is_some(),
        "image/gif" => complete_gif(bytes).is_some(),
        "image/webp" => {
            bytes
                .get(4..8)
                .and_then(|v| v.try_into().ok())
                .is_some_and(|size| u64::from(u32::from_le_bytes(size)) + 8 == bytes.len() as u64)
                && complete_webp_chunks(&bytes[12..], false) == Some(true)
        }
        _ => false,
    };
    if complete {
        Ok(media)
    } else {
        Err(invalid("IMAGE_CONTAINER_TRUNCATED"))
    }
}

fn complete_png(bytes: &[u8]) -> Option<()> {
    let mut at = 8usize;
    let mut has_data = false;
    loop {
        let size = usize::try_from(be32(bytes, at)?).ok()?;
        let end = at.checked_add(size)?.checked_add(12)?;
        bytes.get(..end)?;
        match bytes.get(at + 4..at + 8)? {
            b"IDAT" => has_data |= size > 0,
            b"IEND" => {
                return (has_data
                    && size == 0
                    && end == bytes.len()
                    && bytes.ends_with(b"\0\0\0\0IEND\xaeB`\x82"))
                .then_some(());
            }
            _ => {}
        }
        at = end;
    }
}

fn complete_jpeg(bytes: &[u8]) -> Option<()> {
    if !bytes.ends_with(&[0xff, 0xd9]) {
        return None;
    }
    let mut at = 2usize;
    loop {
        if *bytes.get(at)? != 0xff {
            return None;
        }
        while *bytes.get(at)? == 0xff {
            at += 1;
        }
        let marker = *bytes.get(at)?;
        at += 1;
        if marker == 0xd9 {
            return None;
        }
        if marker == 0x01 || (0xd0..=0xd8).contains(&marker) {
            continue;
        }
        let size = usize::from(u16::from_be_bytes(bytes.get(at..at + 2)?.try_into().ok()?));
        if size < 2 {
            return None;
        }
        at = at.checked_add(size)?;
        bytes.get(..at)?;
        if marker == 0xda {
            return (at < bytes.len() - 2).then_some(());
        }
    }
}

fn gif_subblocks(bytes: &[u8], mut at: usize) -> Option<usize> {
    loop {
        let size = usize::from(*bytes.get(at)?);
        at = at.checked_add(size + 1)?;
        bytes.get(..at)?;
        if size == 0 {
            return Some(at);
        }
    }
}

fn gif_color_table(packed: u8) -> usize {
    if packed & 0x80 == 0 {
        0
    } else {
        3 * (1usize << (usize::from(packed & 7) + 1))
    }
}

fn complete_gif(bytes: &[u8]) -> Option<()> {
    let mut at = 13 + gif_color_table(*bytes.get(10)?);
    bytes.get(..at)?;
    let mut has_image = false;
    loop {
        match *bytes.get(at)? {
            0x3b => return (has_image && at + 1 == bytes.len()).then_some(()),
            0x21 => {
                bytes.get(at + 1)?;
                at = gif_subblocks(bytes, at + 2)?;
            }
            0x2c => {
                let width = le16(bytes, at + 5)?;
                let height = le16(bytes, at + 7)?;
                if width == 0 || height == 0 || u64::from(width) * u64::from(height) > MAX_PIXELS {
                    return None;
                }
                at += 10 + gif_color_table(*bytes.get(at + 9)?);
                if !(2..=8).contains(bytes.get(at)?) || *bytes.get(at + 1)? == 0 {
                    return None;
                }
                at = gif_subblocks(bytes, at + 1)?;
                has_image = true;
            }
            _ => return None,
        }
    }
}

fn complete_webp_chunks(mut bytes: &[u8], frame: bool) -> Option<bool> {
    let mut has_image = false;
    while !bytes.is_empty() {
        let kind = bytes.get(..4)?;
        let size = usize::try_from(u32::from_le_bytes(bytes.get(4..8)?.try_into().ok()?)).ok()?;
        let data = bytes.get(8..8usize.checked_add(size)?)?;
        match kind {
            b"VP8 " => {
                if data.len() < 10 || data.get(3..6) != Some(&[0x9d, 1, 0x2a]) {
                    return None;
                }
                has_image = true;
            }
            b"VP8L" => {
                if data.len() < 5 || data.first() != Some(&0x2f) {
                    return None;
                }
                has_image = true;
            }
            b"ANMF" if !frame => {
                if !complete_webp_chunks(data.get(16..)?, true)? {
                    return None;
                }
                has_image = true;
            }
            b"VP8X" if size != 10 => return None,
            b"ANIM" if size != 6 => return None,
            _ => {}
        }
        bytes = bytes.get(8usize.checked_add(size)?.checked_add(size % 2)?..)?;
    }
    Some(has_image)
}

// Only fixed-size fields and bounded marker/chunk walks are inspected. In
// particular PNG iCCP/zTXt/iTXt payloads must never be decompressed just to size
// an image, and a string such as "acTL" inside another chunk is not animation.
fn dimensions(bytes: &[u8]) -> Option<(u32, u32, bool)> {
    if bytes.starts_with(PNG_SIGNATURE) {
        if bytes.get(8..16)? != b"\0\0\0\rIHDR" {
            return None;
        }
        bytes.get(..33)?;
        return Some((be32(bytes, 16)?, be32(bytes, 20)?, animated_png(bytes)));
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return Some((le16(bytes, 6)?, le16(bytes, 8)?, true));
    }
    if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        return webp_dimensions(bytes);
    }
    jpeg_dimensions(bytes).map(|(width, height)| (width, height, false))
}

fn be32(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn le16(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from(u16::from_le_bytes(
        bytes.get(offset..offset + 2)?.try_into().ok()?,
    )))
}

fn le24(bytes: &[u8], offset: usize) -> Option<u32> {
    let data = bytes.get(offset..offset + 3)?;
    Some(u32::from(data[0]) | u32::from(data[1]) << 8 | u32::from(data[2]) << 16)
}

fn webp_dimensions(bytes: &[u8]) -> Option<(u32, u32, bool)> {
    match bytes.get(12..16)? {
        b"VP8X" => Some((
            1 + le24(bytes, 24)?,
            1 + le24(bytes, 27)?,
            bytes.get(20)? & 2 != 0,
        )),
        b"VP8L" if bytes.get(20) == Some(&0x2f) => {
            let data = bytes.get(21..25)?;
            Some((
                1 + (u32::from(data[0]) | (u32::from(data[1]) & 63) << 8),
                1 + (u32::from(data[1]) >> 6
                    | u32::from(data[2]) << 2
                    | (u32::from(data[3]) & 15) << 10),
                false,
            ))
        }
        b"VP8 " if bytes.get(23..26) == Some(&[0x9d, 1, 0x2a]) => {
            Some((le16(bytes, 26)? & 0x3fff, le16(bytes, 28)? & 0x3fff, false))
        }
        _ => None,
    }
}

fn animated_png(bytes: &[u8]) -> bool {
    let mut offset = 8;
    while let Some(length) = be32(bytes, offset) {
        let Some(end) = usize::try_from(length)
            .ok()
            .and_then(|length| offset.checked_add(length)?.checked_add(12))
        else {
            break;
        };
        if end > bytes.len() {
            break;
        }
        match bytes.get(offset + 4..offset + 8) {
            Some(b"acTL") => return true,
            Some(b"IEND") => break,
            _ => offset = end,
        }
    }
    false
}

fn jpeg_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if !bytes.starts_with(&[0xff, 0xd8]) {
        return None;
    }
    let mut offset = 2;
    loop {
        if *bytes.get(offset)? != 0xff {
            return None;
        }
        while bytes.get(offset) == Some(&0xff) {
            offset += 1;
        }
        let marker = *bytes.get(offset)?;
        offset += 1;
        match marker {
            0xd9 | 0xda | 0x00 => return None,
            0x01 | 0xd0..=0xd8 => continue,
            _ => {}
        }
        let length = usize::from(u16::from_be_bytes(
            bytes.get(offset..offset + 2)?.try_into().ok()?,
        ));
        if length < 2 {
            return None;
        }
        let segment = bytes.get(offset..offset.checked_add(length)?)?;
        if matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) {
            let height = u32::from(u16::from_be_bytes(segment.get(3..5)?.try_into().ok()?));
            let width = u32::from(u16::from_be_bytes(segment.get(5..7)?.try_into().ok()?));
            return Some((width, height));
        }
        offset += length;
    }
}

fn invalid(message: &str) -> ProviderError {
    ProviderError::Preflight {
        message: message.into(),
    }
}

struct WireCounter(usize);
impl Write for WireCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        if self.0 > MAX_WIRE {
            return Err(std::io::Error::other("PAYLOAD_TOO_LARGE"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    Root,
    Messages,
    Parts,
    None,
}

fn media_tokens(value: &Value) -> Result<Option<u64>, ProviderError> {
    let encoded = match value["type"].as_str() {
        Some("image")
            if value["source"]["type"] == "base64"
                && value["source"]["media_type"]
                    .as_str()
                    .is_some_and(|mime| mime.starts_with("image/")) =>
        {
            value["source"]["data"].as_str()
        }
        Some(kind @ ("image_url" | "input_image")) => {
            let url = if kind == "image_url" {
                value["image_url"]["url"].as_str()
            } else {
                value["image_url"].as_str()
            }
            .unwrap_or("");
            url.split_once(',')
                .filter(|(prefix, _)| {
                    prefix.starts_with("data:image/") && prefix.ends_with(";base64")
                })
                .map(|(_, data)| data)
        }
        _ => None,
    };
    encoded
        .map(inline_image_tokens)
        .transpose()
        .map(|count| count.map(|count| count.saturating_add(16)))
}

fn text_tokens(text: &str, ratio: f64) -> u64 {
    // Configuration validates the ratio. UTF-16 length preserves the source
    // guard's conservative counting of supplementary Unicode characters.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    {
        ((text.encode_utf16().count() as f64 / ratio)
            .ceil()
            .min(u64::MAX as f64)) as u64
    }
}

fn field_scope(parent: &Value, key: &str, scope: Scope) -> Scope {
    if scope == Scope::Root && matches!(key, "messages" | "input") {
        return Scope::Messages;
    }
    if key != "content" {
        return Scope::None;
    }
    if scope == Scope::Messages
        && matches!(parent["role"].as_str(), Some("user" | "assistant"))
        && parent.get("type").is_none_or(|kind| kind == "message")
        || scope == Scope::Parts && parent["type"] == "tool_result"
    {
        Scope::Parts
    } else {
        Scope::None
    }
}

fn estimate(value: &Value, field: &str, ratio: f64, scope: Scope) -> Result<u64, ProviderError> {
    if scope == Scope::Parts
        && let Some(cost) = media_tokens(value)?
    {
        return Ok(cost);
    }
    match value {
        Value::Null => Ok(0),
        Value::String(text) => Ok(
            if matches!(field, "data" | "base64" | "encrypted_content" | "signature")
                || text.starts_with("data:")
            {
                text.len() as u64
            } else {
                text_tokens(text, ratio)
            },
        ),
        Value::Array(values) => values.iter().try_fold(2u64, |total, item| {
            Ok(total
                .saturating_add(estimate(item, field, ratio, scope)?)
                .saturating_add(1))
        }),
        Value::Object(values) => values.iter().try_fold(2u64, |total, (key, item)| {
            Ok(total
                .saturating_add(text_tokens(key, ratio))
                .saturating_add(estimate(item, key, ratio, field_scope(value, key, scope))?)
                .saturating_add(1))
        }),
        _ => Ok(1),
    }
}

fn input_budget(context: u32, output: u32) -> u32 {
    context
        .saturating_sub(output)
        .saturating_sub(context.div_ceil(20).max(2048))
}

/// Validate the complete serialized request after provider-specific projection.
/// # Errors
/// Fails on wire size, unsafe raster dimensions, or insufficient input capacity.
pub(crate) fn validate(body: &Value, model: &str, output: u32) -> Result<(), ProviderError> {
    serde_json::to_writer(WireCounter(0), body)
        .map_err(|_| invalid("PAYLOAD_TOO_LARGE: model request exceeds 64 MiB"))?;
    let caps = capabilities_for(model);
    let budget = input_budget(caps.context_window, output);
    if budget == 0 {
        return Err(invalid("INVALID_MODEL_BUDGET_CONFIGURATION"));
    }
    let scope = if is_known_model(model) && caps.supports_images {
        Scope::Root
    } else {
        Scope::None
    };
    if estimate(body, "", caps.token_char_ratio, scope)? > u64::from(budget) {
        return Err(invalid(
            "CONTEXT_BUDGET_EXCEEDED: final provider payload does not fit",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_IMAGE, MAX_WIRE, PNG_SIGNATURE, Scope, dimensions, estimate, inline_image_tokens,
        input_budget, validate,
    };
    use base64::Engine as _;
    use serde_json::json;

    fn encode(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }
    fn png() -> Vec<u8> {
        let mut output = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(128, 128)
            .write_to(&mut output, image::ImageFormat::Png)
            .unwrap();
        output.into_inner()
    }
    fn chunk(kind: [u8; 4], data: &[u8]) -> Vec<u8> {
        let mut out = u32::try_from(data.len()).unwrap().to_be_bytes().to_vec();
        out.extend_from_slice(&kind);
        out.extend_from_slice(data);
        out.extend_from_slice(&[0; 4]);
        out
    }

    #[test]
    fn actual_png_jpeg_webp_and_gif_headers_are_bounded_without_decoding_rasters() {
        for format in [
            image::ImageFormat::Png,
            image::ImageFormat::Jpeg,
            image::ImageFormat::WebP,
            image::ImageFormat::Gif,
        ] {
            let mut bytes = std::io::Cursor::new(Vec::new());
            image::DynamicImage::new_rgb8(128, 128)
                .write_to(&mut bytes, format)
                .unwrap();
            let bytes = bytes.into_inner();
            assert_eq!(
                dimensions(&bytes).map(|(w, h, _)| (w, h)),
                Some((128, 128)),
                "{format:?}"
            );
            assert_eq!(
                super::validated_image_dimensions(&bytes).unwrap(),
                (128, 128)
            );
            assert_eq!(
                inline_image_tokens(&encode(&bytes)).unwrap(),
                if format == image::ImageFormat::Gif {
                    encode(&bytes).len() as u64
                } else {
                    1088
                }
            );
        }
    }

    #[test]
    fn complete_raster_containers_preserve_gif_webp_and_reject_truncated_or_header_only_data() {
        for (format, media) in [
            (image::ImageFormat::Png, "image/png"),
            (image::ImageFormat::Jpeg, "image/jpeg"),
            (image::ImageFormat::WebP, "image/webp"),
            (image::ImageFormat::Gif, "image/gif"),
        ] {
            let mut output = std::io::Cursor::new(Vec::new());
            image::DynamicImage::new_rgb8(32, 32)
                .write_to(&mut output, format)
                .unwrap();
            let bytes = output.into_inner();
            assert_eq!(super::complete_image_media_type(&bytes).unwrap(), media);
            assert!(super::complete_image_media_type(&bytes[..bytes.len() - 1]).is_err());
        }
        let bytes = png();
        let fake_png = [bytes[..33].to_vec(), b"\0\0\0\0IEND\xaeB`\x82".to_vec()].concat();
        assert!(super::complete_image_media_type(&fake_png).is_err());
        assert!(super::complete_image_media_type(b"GIF89a\x01\0\x01\0\0\0\0;").is_err());
        let fake_webp = b"RIFF\x16\0\0\0WEBPVP8X\x0a\0\0\0\0\0\0\0\x01\0\0\x01\0\0";
        assert!(super::complete_image_media_type(fake_webp).is_err());
    }

    #[test]
    fn raster_metadata_is_not_decompressed_and_animation_is_structural() {
        let image = png();
        let mut bytes = image[..33].to_vec();
        bytes.extend(chunk(*b"iCCP", b"not-expanded\0\0malformed-zlib-stream"));
        bytes.extend(chunk(*b"tEXt", b"ordinary acTL and ANIM strings"));
        bytes.extend_from_slice(&image[33..]);
        assert_eq!(inline_image_tokens(&encode(&bytes)).unwrap(), 1088);
        let mut animated = bytes[..33].to_vec();
        animated.extend(chunk(*b"acTL", &[0, 0, 0, 2, 0, 0, 0, 0]));
        animated.extend_from_slice(&bytes[33..]);
        let encoded = encode(&animated);
        assert_eq!(inline_image_tokens(&encoded).unwrap(), encoded.len() as u64);
    }

    #[test]
    fn huge_dimensions_rejected_from_small_headers_and_unknown_bytes_stay_conservative() {
        let mut png_header = PNG_SIGNATURE.to_vec();
        let mut header = 100_000u32.to_be_bytes().to_vec();
        header.extend(100_000u32.to_be_bytes());
        header.extend([8, 2, 0, 0, 0]);
        png_header.extend(chunk(*b"IHDR", &header));
        let mut gif = b"GIF89a".to_vec();
        gif.extend([0xff; 4]);
        for bytes in [png_header, gif] {
            assert!(
                inline_image_tokens(&encode(&bytes))
                    .unwrap_err()
                    .to_string()
                    .contains("IMAGE_PIXEL_LIMIT_EXCEEDED")
            );
        }
        for data in ["", "!invalid!", "dW5rbm93bg=="] {
            assert_eq!(inline_image_tokens(data).unwrap(), data.len() as u64);
        }
        assert!(inline_image_tokens(&"A".repeat(MAX_IMAGE.div_ceil(3) * 4 + 1)).is_err());
        assert!(inline_image_tokens(&encode(&vec![0; MAX_IMAGE + 1])).is_err());
    }

    #[test]
    fn webp_extended_animation_and_truncated_markers_do_not_escape_header_checks() {
        let mut webp = b"RIFF\0\0\0\0WEBPVP8X\x0a\0\0\0\x02\0\0\0\x7f\0\0\x7f\0\0".to_vec();
        assert_eq!(dimensions(&webp), Some((128, 128, true)));
        let encoded = encode(&webp);
        assert_eq!(inline_image_tokens(&encoded).unwrap(), encoded.len() as u64);
        webp[24..27].copy_from_slice(&[0xff; 3]);
        webp[27..30].copy_from_slice(&[0xff; 3]);
        assert!(inline_image_tokens(&encode(&webp)).is_err());
        for bytes in [
            &b"\xff\xd8\xff\xe1\0\0"[..],
            &b"\xff\xd8\xff\xc0\xff\xff"[..],
            &b"\x89PNG\r\n\x1a\n"[..],
        ] {
            assert!(dimensions(bytes).is_none());
        }
    }

    #[test]
    fn only_real_media_parts_receive_raster_estimate() {
        let data = encode(&png());
        let part =
            json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":data}});
        assert_eq!(estimate(&part, "", 3.5, Scope::Parts).unwrap(), 1104);
        let direct = json!({"messages":[{"role":"user","content":[part]}]});
        let nested = json!({"messages":[{"role":"user","content":[{"type":"tool_result","content":[part]}]}]});
        assert!(estimate(&direct, "", 3.5, Scope::Root).unwrap() < 1200);
        assert!(estimate(&nested, "", 3.5, Scope::Root).unwrap() < 1250);
        for fake in [
            json!({"messages":[{"role":"assistant","content":[{"type":"tool_use","input":part}]}]}),
            json!({"tools":[{"input_schema":part}]}),
            json!({"metadata":{"messages":[{"role":"user","content":[part]}]}}),
            json!({"messages":[{"role":"system","content":[part]}]}),
        ] {
            assert!(estimate(&fake, "", 3.5, Scope::Root).unwrap() > data.len() as u64);
        }
        for part in [
            json!({"type":"image_url","image_url":{"url":format!("data:image/png;base64,{data}")}}),
            json!({"type":"input_image","image_url":format!("data:image/png;base64,{data}")}),
        ] {
            assert_eq!(estimate(&part, "", 3.5, Scope::Parts).unwrap(), 1104);
        }
        let wrong = json!({"type":"image_url","image_url":format!("data:image/png;base64,{data}")});
        assert!(estimate(&wrong, "", 3.5, Scope::Parts).unwrap() > data.len() as u64);
    }

    #[test]
    fn opaque_signature_and_unicode_use_conservative_counts() {
        assert_eq!(
            estimate(&json!("x".repeat(4096)), "signature", 4.0, Scope::None).unwrap(),
            4096
        );
        assert_eq!(
            estimate(&json!("😀😀"), "content", 2.0, Scope::None).unwrap(),
            2
        );
        assert_eq!(input_budget(8192, 4096), 2048);
        assert_eq!(input_budget(40_001, 1000), 36_953);
        assert_eq!(input_budget(100_001, 4096), 90_904);
        assert!(
            validate(
                &json!({"messages":[{"role":"user","content":"x".repeat(10_000)}]}),
                "unknown",
                4096
            )
            .unwrap_err()
            .to_string()
            .contains("CONTEXT_BUDGET_EXCEEDED")
        );
        assert!(
            validate(&json!({}), "unknown", 8192)
                .unwrap_err()
                .to_string()
                .contains("INVALID_MODEL_BUDGET_CONFIGURATION")
        );
    }

    #[test]
    fn wire_limit_is_independent_of_raster_and_character_estimates() {
        assert!(
            validate(
                &json!({"prompt":"x".repeat(MAX_WIRE)}),
                "deepseek-flash",
                8192
            )
            .unwrap_err()
            .to_string()
            .contains("PAYLOAD_TOO_LARGE")
        );
    }
}
