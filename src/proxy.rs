use anyhow::{Context, Result, bail};
use grammers_mtsender::ConnectionParams;
use std::net::IpAddr;

type ProxyCredentials = (String, String);
type ProxyParts = (String, u16, Option<ProxyCredentials>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProxyKind {
    Socks5,
    Http,
}

impl ProxyKind {
    pub const fn scheme(self) -> &'static str {
        match self {
            Self::Socks5 => "socks5",
            Self::Http => "http",
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::Socks5 => "SOCKS5",
            Self::Http => "HTTP",
        }
    }
}

pub fn parse_proxy_input(kind: ProxyKind, value: &str) -> Result<String> {
    let value = trim_matching_quotes(value.trim());
    if value.is_empty() {
        bail!("proxy must not be empty");
    }
    if value.contains("://") {
        validate_proxy_url(value)?;
        let actual = proxy_kind(value).context("proxy type is missing")?;
        if actual != kind {
            bail!(
                "selected {} proxy but the supplied value is {}",
                kind.label(),
                actual.label()
            );
        }
        return Ok(value.trim_end_matches('/').to_owned());
    }

    let (host, port, credentials) = parse_shorthand(value)?;
    let authority_host = match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(address)) => address.to_string(),
        Ok(IpAddr::V6(address)) => format!("[{address}]"),
        Err(_) => match url::Host::parse(&host).context("proxy host/IP is invalid")? {
            url::Host::Domain(domain) => domain,
            url::Host::Ipv4(address) => address.to_string(),
            url::Host::Ipv6(address) => format!("[{address}]"),
        },
    };
    let mut proxy = url::Url::parse(&format!("{}://{authority_host}:{port}", kind.scheme()))
        .context("failed to construct proxy configuration")?;
    if let Some((username, password)) = credentials {
        proxy
            .set_username(&username)
            .map_err(|_| anyhow::anyhow!("proxy username is invalid"))?;
        proxy
            .set_password(Some(&password))
            .map_err(|_| anyhow::anyhow!("proxy password is invalid"))?;
    }
    let canonical = proxy.as_str().trim_end_matches('/').to_owned();
    validate_proxy_url(&canonical)?;
    Ok(canonical)
}

fn parse_shorthand(value: &str) -> Result<ProxyParts> {
    let fields = if value.starts_with('[') {
        let closing = value
            .find(']')
            .context("IPv6 proxy addresses must close with ']'")?;
        let host = &value[1..closing];
        let tail = value[closing + 1..]
            .strip_prefix(':')
            .context("proxy port must follow the closing ']'")?;
        let mut fields = vec![host.to_owned()];
        fields.extend(tail.splitn(3, ':').map(str::to_owned));
        fields
    } else {
        value.splitn(4, ':').map(str::to_owned).collect()
    };

    if fields.len() != 2 && fields.len() != 4 {
        bail!("use IP:port or IP:port:username:password");
    }
    let host = fields[0].trim();
    if host.is_empty() || host.chars().any(char::is_whitespace) {
        bail!("proxy host/IP must not be empty or contain spaces");
    }
    let port = fields[1]
        .parse::<u16>()
        .context("proxy port must be a number from 1 to 65535")?;
    if port == 0 {
        bail!("proxy port must be between 1 and 65535");
    }
    let credentials = if fields.len() == 4 {
        let username = fields[2].trim();
        let password = fields[3].trim();
        if username.is_empty() || password.is_empty() {
            bail!("proxy username and password must both be provided");
        }
        Some((username.to_owned(), password.to_owned()))
    } else {
        None
    };
    Ok((host.to_owned(), port, credentials))
}

pub fn validate_proxy_url(value: &str) -> Result<()> {
    let proxy = url::Url::parse(value.trim()).context("proxy configuration is invalid")?;
    if !matches!(proxy.scheme(), "socks5" | "http") {
        bail!("proxy type must be SOCKS5 or HTTP");
    }
    if proxy.host().is_none() {
        bail!("proxy must contain a host/IP");
    }
    if proxy.port().is_none() {
        bail!("proxy must contain an explicit port");
    }
    if proxy.password().is_some() && proxy.username().is_empty() {
        bail!("proxy password requires a username");
    }
    if proxy.query().is_some() || proxy.fragment().is_some() {
        bail!("proxy must not contain a query string or fragment");
    }
    if !matches!(proxy.path(), "" | "/") {
        bail!("proxy must not contain a path");
    }
    Ok(())
}

pub fn proxy_kind(value: &str) -> Option<ProxyKind> {
    match url::Url::parse(value).ok()?.scheme() {
        "socks5" => Some(ProxyKind::Socks5),
        "http" => Some(ProxyKind::Http),
        _ => None,
    }
}

pub fn proxy_label(proxy_url: Option<&str>) -> &'static str {
    proxy_url
        .and_then(proxy_kind)
        .map(ProxyKind::label)
        .unwrap_or("DIRECT")
}

pub fn connection_params(proxy_url: Option<&str>) -> ConnectionParams {
    ConnectionParams {
        proxy_url: proxy_url.map(str::to_owned),
        ..Default::default()
    }
}

fn trim_matching_quotes(value: &str) -> &str {
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        &value[1..value.len() - 1]
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_shorthand_for_both_proxy_types() -> Result<()> {
        assert_eq!(
            parse_proxy_input(ProxyKind::Socks5, "127.0.0.1:1080")?,
            "socks5://127.0.0.1:1080"
        );
        assert_eq!(
            parse_proxy_input(ProxyKind::Http, "proxy.example:8080:user:pass")?,
            "http://user:pass@proxy.example:8080"
        );
        assert_eq!(
            parse_proxy_input(ProxyKind::Http, "\"[::1]:8080\"")?,
            "http://[::1]:8080"
        );
        Ok(())
    }

    #[test]
    fn safely_encodes_proxy_credentials() -> Result<()> {
        let proxy = parse_proxy_input(ProxyKind::Socks5, "127.0.0.1:1080:user@example:p@ss:word")?;
        assert_eq!(
            proxy,
            "socks5://user%40example:p%40ss%3Aword@127.0.0.1:1080"
        );
        Ok(())
    }

    #[test]
    fn accepts_canonical_config_values_for_backward_compatibility() {
        assert!(validate_proxy_url("socks5://127.0.0.1:1080").is_ok());
        assert!(validate_proxy_url("http://user:pass@example.com:8080").is_ok());
    }

    #[test]
    fn rejects_incomplete_or_ambiguous_shorthand() {
        assert!(parse_proxy_input(ProxyKind::Http, "127.0.0.1").is_err());
        assert!(parse_proxy_input(ProxyKind::Http, "127.0.0.1:8080:user").is_err());
        assert!(parse_proxy_input(ProxyKind::Http, "127.0.0.1:0").is_err());
        assert!(parse_proxy_input(ProxyKind::Http, "http://127.0.0.1:8080/path").is_err());
        assert!(parse_proxy_input(ProxyKind::Http, "socks5://127.0.0.1:1080").is_err());
    }
}
