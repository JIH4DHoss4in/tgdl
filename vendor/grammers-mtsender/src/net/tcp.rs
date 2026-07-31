// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use log::info;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
pub use tokio::net::tcp::{ReadHalf, WriteHalf};

use super::ServerAddr;

pub enum NetStream {
    Tcp(TcpStream),
    #[cfg(feature = "proxy")]
    ProxySocks5(tokio_socks::tcp::Socks5Stream<TcpStream>),
}

impl NetStream {
    pub(crate) fn split(&mut self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        match self {
            Self::Tcp(stream) => stream.split(),
            #[cfg(feature = "proxy")]
            Self::ProxySocks5(stream) => stream.split(),
        }
    }

    pub(crate) async fn connect(addr: &ServerAddr) -> Result<Self, std::io::Error> {
        info!("connecting...");
        match addr {
            ServerAddr::Tcp { address } => Ok(NetStream::Tcp(TcpStream::connect(address).await?)),
            #[cfg(feature = "proxy")]
            ServerAddr::Proxied { address, proxy } => {
                Self::connect_proxy_stream(address, proxy).await
            }
        }
    }

    #[cfg(feature = "proxy")]
    async fn connect_proxy_stream(
        addr: &std::net::SocketAddr,
        proxy_url: &str,
    ) -> Result<NetStream, std::io::Error> {
        use std::{
            io::{self, ErrorKind},
            net::{IpAddr, SocketAddr},
        };

        use hickory_resolver::Resolver;
        use url::Host;

        let proxy = url::Url::parse(proxy_url)
            .map_err(|err| io::Error::new(ErrorKind::InvalidData, err))?;
        let scheme = proxy.scheme();
        let host = proxy.host().ok_or(io::Error::new(
            ErrorKind::NotFound,
            "proxy host is missing from URL",
        ))?;
        let port = proxy.port().ok_or(io::Error::new(
            ErrorKind::NotFound,
            "proxy port is missing from URL",
        ))?;
        let username = percent_decode_userinfo(proxy.username())?;
        let password = percent_decode_userinfo(proxy.password().unwrap_or(""))?;
        let proxy_addr = match host {
            Host::Domain(domain) => {
                let resolver = Resolver::builder_tokio()
                    .map_err(io::Error::other)?
                    .build()
                    .map_err(io::Error::other)?;
                let response = resolver
                    .lookup_ip(domain)
                    .await
                    .map_err(|err| io::Error::new(ErrorKind::Other, err))?;
                let socks_ip_addr = response.iter().next().ok_or(io::Error::new(
                    ErrorKind::NotFound,
                    format!("proxy host did not return any ip address: {}", domain),
                ))?;
                SocketAddr::new(socks_ip_addr, port)
            }
            Host::Ipv4(v4) => SocketAddr::new(IpAddr::from(v4), port),
            Host::Ipv6(v6) => SocketAddr::new(IpAddr::from(v6), port),
        };

        match scheme {
            "socks5" => {
                if username.is_empty() {
                    Ok(NetStream::ProxySocks5(
                        tokio_socks::tcp::Socks5Stream::connect(proxy_addr, addr)
                            .await
                            .map_err(|err| io::Error::new(ErrorKind::ConnectionAborted, err))?,
                    ))
                } else {
                    Ok(NetStream::ProxySocks5(
                        tokio_socks::tcp::Socks5Stream::connect_with_password(
                            proxy_addr, addr, &username, &password,
                        )
                        .await
                        .map_err(|err| io::Error::new(ErrorKind::ConnectionAborted, err))?,
                    ))
                }
            }
            "http" => Self::connect_http_proxy(proxy_addr, addr, &username, &password).await,
            scheme => Err(io::Error::new(
                ErrorKind::ConnectionAborted,
                format!("proxy scheme not supported: {}", scheme),
            )),
        }
    }

    #[cfg(feature = "proxy")]
    async fn connect_http_proxy(
        proxy_addr: std::net::SocketAddr,
        target_addr: &std::net::SocketAddr,
        username: &str,
        password: &str,
    ) -> Result<NetStream, std::io::Error> {
        use std::io::{self, ErrorKind};

        if username.contains('\r')
            || username.contains('\n')
            || password.contains('\r')
            || password.contains('\n')
        {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "HTTP proxy credentials contain a line break",
            ));
        }

        let mut stream = TcpStream::connect(proxy_addr).await?;
        let mut request = format!(
            "CONNECT {target_addr} HTTP/1.1\r\nHost: {target_addr}\r\n\
             Proxy-Connection: Keep-Alive\r\n"
        );
        if !username.is_empty() {
            let credentials = encode_base64(format!("{username}:{password}").as_bytes());
            request.push_str("Proxy-Authorization: Basic ");
            request.push_str(&credentials);
            request.push_str("\r\n");
        }
        request.push_str("\r\n");
        stream.write_all(request.as_bytes()).await?;
        stream.flush().await?;

        const MAX_RESPONSE_HEADER: usize = 16 * 1024;
        let mut response = Vec::with_capacity(256);
        let mut byte = [0_u8; 1];
        while !response.ends_with(b"\r\n\r\n") {
            if response.len() >= MAX_RESPONSE_HEADER {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "HTTP proxy response header exceeded 16 KiB",
                ));
            }
            stream.read_exact(&mut byte).await?;
            response.push(byte[0]);
        }
        validate_http_connect_response(&response)?;
        Ok(NetStream::Tcp(stream))
    }

    pub(crate) async fn disconnect(&mut self) -> std::io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.shutdown().await,
            #[cfg(feature = "proxy")]
            Self::ProxySocks5(stream) => stream.shutdown().await,
        }
    }
}

#[cfg(feature = "proxy")]
fn validate_http_connect_response(response: &[u8]) -> Result<(), std::io::Error> {
    use std::io::{self, ErrorKind};

    let response = std::str::from_utf8(response)
        .map_err(|error| io::Error::new(ErrorKind::InvalidData, error))?;
    let status_line = response
        .split("\r\n")
        .next()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "HTTP proxy response was empty"))?;
    let mut parts = status_line.split_whitespace();
    let protocol = parts.next().unwrap_or_default();
    let status = parts.next().unwrap_or_default();
    if !protocol.starts_with("HTTP/") {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "HTTP proxy returned an invalid status line",
        ));
    }
    if status != "200" {
        let kind = if status == "407" {
            ErrorKind::PermissionDenied
        } else {
            ErrorKind::ConnectionRefused
        };
        return Err(io::Error::new(
            kind,
            format!("HTTP proxy CONNECT failed with status {status}"),
        ));
    }
    Ok(())
}

#[cfg(feature = "proxy")]
fn percent_decode_userinfo(value: &str) -> Result<String, std::io::Error> {
    use std::io::{self, ErrorKind};

    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let high = bytes.get(index + 1).and_then(|value| hex_value(*value));
            let low = bytes.get(index + 2).and_then(|value| hex_value(*value));
            let (Some(high), Some(low)) = (high, low) else {
                return Err(io::Error::new(
                    ErrorKind::InvalidData,
                    "HTTP proxy credentials contain invalid percent encoding",
                ));
            };
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|error| io::Error::new(ErrorKind::InvalidData, error))
}

#[cfg(feature = "proxy")]
fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(feature = "proxy")]
fn encode_base64(value: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(value.len().div_ceil(3) * 4);
    for chunk in value.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        encoded.push(TABLE[(first >> 2) as usize] as char);
        encoded.push(TABLE[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(TABLE[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(TABLE[(third & 0x3f) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

#[cfg(all(test, feature = "proxy"))]
mod tests {
    use super::*;

    #[test]
    fn builds_http_basic_credentials() {
        assert_eq!(encode_base64(b"user:password"), "dXNlcjpwYXNzd29yZA==");
        assert_eq!(encode_base64(b"a:b"), "YTpi");
    }

    #[test]
    fn decodes_url_userinfo() {
        assert_eq!(
            percent_decode_userinfo("user%40example.com").unwrap(),
            "user@example.com"
        );
        assert!(percent_decode_userinfo("bad%2").is_err());
    }

    #[test]
    fn accepts_only_successful_connect_responses() {
        assert!(
            validate_http_connect_response(b"HTTP/1.1 200 Connection Established\r\n\r\n").is_ok()
        );
        assert!(
            validate_http_connect_response(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
                .is_err()
        );
        assert!(validate_http_connect_response(b"not HTTP\r\n\r\n").is_err());
    }

    #[tokio::test]
    async fn establishes_an_authenticated_http_connect_tunnel() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let target_addr = "149.154.167.91:443".parse().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut byte = [0_u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("CONNECT 149.154.167.91:443 HTTP/1.1\r\n"));
            assert!(request.contains("Proxy-Authorization: Basic dXNlcjpwYXNz\r\n"));
            socket
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            let mut payload = [0_u8; 4];
            socket.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, b"ping");
            socket.write_all(b"pong").await.unwrap();
        });

        let mut stream = NetStream::connect_http_proxy(proxy_addr, &target_addr, "user", "pass")
            .await
            .unwrap();
        match &mut stream {
            NetStream::Tcp(stream) => {
                stream.write_all(b"ping").await.unwrap();
                let mut response = [0_u8; 4];
                stream.read_exact(&mut response).await.unwrap();
                assert_eq!(&response, b"pong");
            }
            #[cfg(feature = "proxy")]
            NetStream::ProxySocks5(_) => panic!("expected an HTTP tunnel"),
        }
        server.await.unwrap();
    }
}
