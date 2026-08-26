//! 剪贴板图片 URL 信任校验（Phase A7；对照旧
//! `OssPublishProperties.isTrustedClipboardImageUrl` L114-131——OSS 发布链路
//! 其余能力不在本端范围，仅抄录校验规则）。
//!
//! 七连判定（Java 原文顺序）：scheme 为 `https`（大小写不敏感）、host 精确
//! 等于 `{bucket 小写}.{endpoint host}`（大小写不敏感）、无显式端口、无
//! userinfo、无 query / fragment、rawPath 以 `/{prefix}/clipboard/` 开头、
//! rawPath 不含 `%` 且规范化（消解 dot 段）后不变；任何解析异常一律 `false`。
//!
//! # 与 Java `URI` 的语义差异及处置
//!
//! `url` crate 是浏览器式容错解析器，会**静默规范化**若干 Java `URI` 会如实
//! 暴露（进而被判定拒绝）的输入：剥离默认端口 `:443`、消解 `.` / `..` 段、
//! 容忍内嵌空白。故显式端口 / userinfo / rawPath 三组判定在**原始字符串**上
//! 先行完成——只会更严格，fail-closed 方向安全。

/// 可信剪贴板图片 URL 策略（组合根构造一次，供引擎逐 url 校验）。
#[derive(Clone, Debug)]
pub struct TrustedImageUrlPolicy {
    /// 期望 host：`{bucket 小写}.{endpoint host}`；OSS 未配置或 endpoint
    /// 解析不出 host → `None`（策略恒拒绝——fail-closed）。
    expected_host: Option<String>,
    /// 期望路径前缀：`/{prefix}/clipboard/`（旧 `expectedPath`）。
    expected_path: String,
}

impl TrustedImageUrlPolicy {
    /// 由配置装配（旧 `OssPublishProperties` 的 `endpoint` / `bucket` /
    /// `prefix` 三项）。`endpoint` / `bucket` 缺失、空白或 endpoint 解析
    /// 失败 → 恒拒绝策略。
    #[must_use]
    pub fn from_settings(endpoint: Option<&str>, bucket: Option<&str>, prefix: &str) -> Self {
        let expected_host = match (endpoint, bucket) {
            (Some(endpoint), Some(bucket))
                if !endpoint.trim().is_empty() && !bucket.trim().is_empty() =>
            {
                endpoint_host(endpoint.trim())
                    .map(|host| format!("{}.{host}", bucket.trim().to_lowercase()))
            }
            _ => None,
        };
        Self {
            expected_host,
            expected_path: format!("/{}/clipboard/", normalized_prefix(prefix)),
        }
    }

    /// 是否为当前服务签发的可信剪贴板图片地址（旧
    /// `isTrustedClipboardImageUrl` 逐条对照；SSRF 红线，校验必须先于
    /// 任何 url 附件受理）。
    #[must_use]
    pub fn is_trusted_clipboard_image_url(&self, value: &str) -> bool {
        let Some(expected_host) = self.expected_host.as_deref() else {
            return false;
        };
        if value.trim().is_empty() {
            return false;
        }
        // Java `URI.create` 对内嵌空白 / 控制字符抛异常 → false；`url`
        // crate 会静默剔除，须在原始串上先行拒绝。
        if value
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control())
        {
            return false;
        }
        // 原始串前缀必须逐字为 `https://`（大小写不敏感）——排除
        // `https:/`、`https:\\` 等 `url` crate 会容错补齐的形态。
        let Some(rest) = strip_https_scheme(value) else {
            return false;
        };
        // 原始 authority：显式端口（含 `:443`）与 userinfo 判定（旧
        // `getPort() == -1 && getUserInfo() == null`；`url` crate 会静默
        // 剥离默认端口与空 userinfo，故在原始串上判定）。
        let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
        if authority.contains(':') || authority.contains('@') {
            return false;
        }
        // 原始 rawPath 三连判定（旧 `startsWith` / `contains("%")` /
        // `equals(uri.normalize().getRawPath())`——dot 段规范化后必变）。
        let raw_path = rest[authority.len()..]
            .split(['?', '#'])
            .next()
            .unwrap_or("");
        if !raw_path.starts_with(&self.expected_path) || raw_path.contains('%') {
            return false;
        }
        if raw_path
            .split('/')
            .any(|segment| segment == "." || segment == "..")
        {
            return false;
        }
        // 结构化二次校验：host 精确匹配 + query / fragment 缺席。
        let Ok(url) = url::Url::parse(value) else {
            return false;
        };
        url.scheme() == "https"
            && url
                .host_str()
                .is_some_and(|host| host.eq_ignore_ascii_case(expected_host))
            && url.port().is_none()
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
    }
}

/// endpoint 的 host 部分（旧 `endpointUri().getHost()`——Java `URI` 要求
/// endpoint 携带 scheme，否则 `getHost()` 为 null → 恒拒绝）。
fn endpoint_host(endpoint: &str) -> Option<String> {
    let url = url::Url::parse(endpoint).ok()?;
    url.host_str().map(str::to_owned)
}

/// 前缀规范化（旧 `normalizedPrefix`：trim、`\` → `/`、去尾部 `/`）。
fn normalized_prefix(prefix: &str) -> String {
    let mut value = prefix.trim().replace('\\', "/");
    while value.ends_with('/') {
        value.pop();
    }
    value
}

/// 剥离逐字 `https://` 前缀（大小写不敏感），返回 authority 起的剩余部分。
fn strip_https_scheme(value: &str) -> Option<&str> {
    let (scheme, rest) = value.split_once("://")?;
    scheme.eq_ignore_ascii_case("https").then_some(rest)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 标准配置策略（endpoint / bucket / prefix 皆为旧系统缺省风格）。
    fn policy() -> TrustedImageUrlPolicy {
        TrustedImageUrlPolicy::from_settings(
            Some("https://oss-cn-hangzhou.aliyuncs.com"),
            Some("Bkt"),
            "zhikuncode-artifacts",
        )
    }

    const TRUSTED: &str =
        "https://bkt.oss-cn-hangzhou.aliyuncs.com/zhikuncode-artifacts/clipboard/a.png";

    #[test]
    fn trusted_clipboard_url_passes() {
        assert!(policy().is_trusted_clipboard_image_url(TRUSTED));
        // scheme / host 大小写不敏感（旧 equalsIgnoreCase）。
        assert!(policy().is_trusted_clipboard_image_url(
            "HTTPS://BKT.OSS-CN-HANGZHOU.ALIYUNCS.COM/zhikuncode-artifacts/clipboard/a.png"
        ));
        // 子路径同样可信（前缀判定）。
        assert!(policy().is_trusted_clipboard_image_url(
            "https://bkt.oss-cn-hangzhou.aliyuncs.com/zhikuncode-artifacts/clipboard/deep/b.jpg"
        ));
    }

    #[test]
    fn non_https_and_host_spoofing_are_rejected() {
        let policy = policy();
        // 非 https。
        assert!(!policy.is_trusted_clipboard_image_url(
            "http://bkt.oss-cn-hangzhou.aliyuncs.com/zhikuncode-artifacts/clipboard/a.png"
        ));
        // host 后缀伪装（可信 host 作子域前缀）。
        assert!(!policy.is_trusted_clipboard_image_url(
            "https://bkt.oss-cn-hangzhou.aliyuncs.com.evil.com/zhikuncode-artifacts/clipboard/a.png"
        ));
        // host 前缀伪装。
        assert!(!policy.is_trusted_clipboard_image_url(
            "https://evil-bkt.oss-cn-hangzhou.aliyuncs.com/zhikuncode-artifacts/clipboard/a.png"
        ));
        // userinfo 伪装（`@` 前是假 host）。
        assert!(!policy.is_trusted_clipboard_image_url(
            "https://bkt.oss-cn-hangzhou.aliyuncs.com@evil.com/zhikuncode-artifacts/clipboard/a.png"
        ));
        // 显式端口（含默认 443——旧 `getPort() == -1` 同样拒绝）。
        assert!(!policy.is_trusted_clipboard_image_url(
            "https://bkt.oss-cn-hangzhou.aliyuncs.com:443/zhikuncode-artifacts/clipboard/a.png"
        ));
    }

    #[test]
    fn path_traversal_encoding_and_extras_are_rejected() {
        let policy = policy();
        // dot 段穿越（规范化后路径必变）。
        assert!(!policy.is_trusted_clipboard_image_url(
            "https://bkt.oss-cn-hangzhou.aliyuncs.com/zhikuncode-artifacts/clipboard/../secret.png"
        ));
        // `%` 编码（旧 `!rawPath.contains("%")`）。
        assert!(!policy.is_trusted_clipboard_image_url(
            "https://bkt.oss-cn-hangzhou.aliyuncs.com/zhikuncode-artifacts/clipboard/%2e%2e/a.png"
        ));
        // 错误路径前缀。
        assert!(!policy.is_trusted_clipboard_image_url(
            "https://bkt.oss-cn-hangzhou.aliyuncs.com/other-prefix/clipboard/a.png"
        ));
        assert!(!policy.is_trusted_clipboard_image_url(
            "https://bkt.oss-cn-hangzhou.aliyuncs.com/zhikuncode-artifacts/other/a.png"
        ));
        // query / fragment（旧 `getQuery() == null && getFragment() == null`）。
        assert!(!policy.is_trusted_clipboard_image_url(&format!("{TRUSTED}?x=1")));
        assert!(!policy.is_trusted_clipboard_image_url(&format!("{TRUSTED}#frag")));
        // 空白 / 垃圾输入（旧 `URI.create` 异常分支）。
        assert!(!policy.is_trusted_clipboard_image_url(""));
        assert!(!policy.is_trusted_clipboard_image_url("   "));
        assert!(!policy.is_trusted_clipboard_image_url("not a url"));
        assert!(!policy.is_trusted_clipboard_image_url(
            "https://bkt.oss-cn-hangzhou.aliyuncs.com/zhikuncode-artifacts/clipboard/a b.png"
        ));
    }

    #[test]
    fn unconfigured_policy_rejects_everything() {
        // OSS 未配置（endpoint / bucket 任一缺失）→ 恒拒绝（fail-closed）。
        let unconfigured = TrustedImageUrlPolicy::from_settings(None, None, "zhikuncode-artifacts");
        assert!(!unconfigured.is_trusted_clipboard_image_url(TRUSTED));
        let half = TrustedImageUrlPolicy::from_settings(
            Some("https://oss-cn-hangzhou.aliyuncs.com"),
            None,
            "zhikuncode-artifacts",
        );
        assert!(!half.is_trusted_clipboard_image_url(TRUSTED));
        // endpoint 无 scheme：Java `URI.getHost()` 为 null → 恒拒绝。
        let no_scheme = TrustedImageUrlPolicy::from_settings(
            Some("oss-cn-hangzhou.aliyuncs.com"),
            Some("bkt"),
            "zhikuncode-artifacts",
        );
        assert!(!no_scheme.is_trusted_clipboard_image_url(TRUSTED));
    }
}
