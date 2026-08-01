use anyhow::Result;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, USER_AGENT},
    Client, ClientBuilder, Proxy, Response,
};
use std::collections::HashMap;
use std::time::Duration;

use crate::types::HttpMethod;

/// Extrahiert den Wert eines Attributs aus einem Element basierend auf dem Selektortyp

static CLIENT_POOL: Lazy<DashMap<String, Client>> = Lazy::new(DashMap::new);

fn build_client(
    proxy: Option<&str>,
    user_agent: Option<&str>,
    timeout_secs: u64,
) -> Result<Client> {
    let mut builder = ClientBuilder::new()
        .timeout(Duration::from_secs(timeout_secs))
        .pool_max_idle_per_host(32)
        .tcp_keepalive(Duration::from_secs(30))
        .pool_idle_timeout(Duration::from_secs(90))
        .http2_adaptive_window(true)
        .http2_initial_connection_window_size(1024 * 1024 * 16)
        .brotli(true)
        .gzip(true);

    let mut headers = HeaderMap::new();
    headers.insert(
        USER_AGENT,
        HeaderValue::from_str(user_agent.unwrap_or("DataScraper/1.0"))?,
    );
    headers.insert("Accept", HeaderValue::from_str("*/*")?);
    headers.insert(
        "Accept-Language",
        HeaderValue::from_str("en-US,en;q=0.9")?,
    );
    builder = builder.default_headers(headers);

    if let Some(proxy_url) = proxy {
        builder = builder.proxy(Proxy::all(proxy_url)?);
    }

    Ok(builder.build()?)
}

pub fn get_client(
    proxy: Option<&str>,
    user_agent: Option<&str>,
    timeout_secs: u64,
) -> Client {
    let key = format!("{:?}|{:?}|{}", proxy, user_agent, timeout_secs);
    if let Some(client) = CLIENT_POOL.get(&key) {
        return client.clone();
    }
    if let Ok(client) = build_client(proxy, user_agent, timeout_secs) {
        CLIENT_POOL.insert(key.clone(), client.clone());
        return client;
    }
    CLIENT_POOL
        .get(&key)
        .map(|c| c.clone())
        .unwrap_or_else(|| ClientBuilder::new().build().unwrap())
}

pub async fn fetch_url(
    url: &str,
    proxy: Option<&str>,
    user_agent: Option<&str>,
    timeout_secs: u64,
    method: &HttpMethod,
    body: Option<&str>,
    headers: &HashMap<String, String>,
) -> Result<Response> {
    let client = get_client(proxy, user_agent, timeout_secs);

    let mut request = match method {
        HttpMethod::Get => client.get(url),
        HttpMethod::Post => client.post(url),
        HttpMethod::Put => client.put(url),
        HttpMethod::Delete => client.delete(url),
    };

    if let Some(body) = body {
        let trimmed = body.trim_start();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            request = request.header(reqwest::header::CONTENT_TYPE, "application/json");
        }
        request = request.body(body.to_string());
    }

    for (key, value) in headers {
        if let (Ok(name), Ok(val)) = (
            HeaderName::try_from(key.as_str()),
            HeaderValue::from_str(value),
        ) {
            request = request.header(name, val);
        }
    }

    Ok(request.send().await?)
}

pub async fn fetch_url_with_retry(
    url: &str,
    proxy: Option<&str>,
    user_agent: Option<&str>,
    timeout_secs: u64,
    max_retries: u32,
    method: &HttpMethod,
    body: Option<&str>,
    headers: &HashMap<String, String>,
) -> Result<Response> {
    let mut last_error = None;
    for attempt in 0..=max_retries {
        match fetch_url(url, proxy, user_agent, timeout_secs, method, body, headers).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    return Ok(resp);
                }
                let status = resp.status();
                let code = status.as_u16();
                // ponytail: 4xx is a client bug, retrying wastes requests and
                // hammers rate-limited APIs; 408/429 are transient, retried
                if code >= 400 && code < 500 && code != 408 && code != 429 {
                    return Err(anyhow::anyhow!(
                        "HTTP {} for {} (client error, not retried)",
                        status,
                        url
                    ));
                }
                let retry_after_secs = resp
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .unwrap_or(0);
                last_error = Some(anyhow::anyhow!("HTTP {} for {}", status, url));
                if attempt < max_retries {
                    sleep_retry_delay(attempt, retry_after_secs).await;
                }
            }
            Err(e) => {
                last_error = Some(e);
                if attempt < max_retries {
                    sleep_retry_delay(attempt, 0).await;
                }
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Request failed after {} retries", max_retries)))
}

// ponytail: ±30% jitter breaks the thundering herd; Retry-After capped at 60s
async fn sleep_retry_delay(attempt: u32, retry_after_secs: u64) {
    let base_ms = 500u64 * 2u64.pow(attempt);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let factor = 0.7 + (nanos % 1000) as f64 / 1000.0 * 0.6;
    let backoff_ms = (base_ms as f64 * factor) as u64;
    let retry_ms = retry_after_secs.min(60).saturating_mul(1000);
    tokio::time::sleep(Duration::from_millis(backoff_ms.max(retry_ms))).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn spawn_status_server(status: &'static str, headers: &'static str, count: Arc<AtomicUsize>) -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                count.fetch_add(1, Ordering::SeqCst);
                let mut stream = stream;
                let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                let mut drain = String::new();
                loop {
                    drain.clear();
                    if reader.read_line(&mut drain).unwrap() == 0 || drain == "\r\n" {
                        break;
                    }
                }
                let body = "nope";
                let resp = format!(
                    "HTTP/1.1 {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n{}\r\nConnection: close\r\n\r\n{}",
                    status,
                    body.len(),
                    headers,
                    body
                );
                let _ = stream.write_all(resp.as_bytes());
            }
        });
        addr
    }

    #[tokio::test]
    async fn four_xx_is_not_retried() {
        let count = Arc::new(AtomicUsize::new(0));
        let addr = spawn_status_server("404 Not Found", "", count.clone());
        let err = fetch_url_with_retry(
            &format!("http://{}/", addr),
            None,
            None,
            10,
            2,
            &HttpMethod::Get,
            None,
            &HashMap::new(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("404"), "{}", err);
        assert_eq!(count.load(Ordering::SeqCst), 1, "4xx must not be retried");
    }

    #[tokio::test]
    async fn five_xx_is_retried_until_success() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let count_srv = count.clone();
        std::thread::spawn(move || {
            for _ in 0..2 {
                if let Ok((stream, _)) = listener.accept() {
                    count_srv.fetch_add(1, Ordering::SeqCst);
                    let mut stream = stream;
                    let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                    let mut drain = String::new();
                    loop {
                        drain.clear();
                        if reader.read_line(&mut drain).unwrap() == 0 || drain == "\r\n" {
                            break;
                        }
                    }
                    let (status, body) = if count_srv.load(Ordering::SeqCst) == 1 {
                        ("500 Internal Server Error", "boom")
                    } else {
                        ("200 OK", "[{\"id\":1}]")
                    };
                    let resp = format!(
                        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        status,
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes());
                }
            }
        });
        let resp = fetch_url_with_retry(
            &format!("http://{}/", addr),
            None,
            None,
            10,
            2,
            &HttpMethod::Get,
            None,
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert!(resp.status().is_success());
        assert_eq!(count.load(Ordering::SeqCst), 2, "5xx must be retried");
    }

    #[tokio::test]
    async fn rate_limit_is_retried() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let count_srv = count.clone();
        std::thread::spawn(move || {
            for _ in 0..2 {
                if let Ok((stream, _)) = listener.accept() {
                    count_srv.fetch_add(1, Ordering::SeqCst);
                    let mut stream = stream;
                    let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                    let mut drain = String::new();
                    loop {
                        drain.clear();
                        if reader.read_line(&mut drain).unwrap() == 0 || drain == "\r\n" {
                            break;
                        }
                    }
                    let (status, body) = if count_srv.load(Ordering::SeqCst) == 1 {
                        ("429 Too Many Requests", "slow down")
                    } else {
                        ("200 OK", "[{\"id\":1}]")
                    };
                    let headers = if status.contains("429") {
                        "Retry-After: 0\r\n"
                    } else {
                        ""
                    };
                    let resp = format!(
                        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{}\r\nConnection: close\r\n\r\n{}",
                        status,
                        body.len(),
                        headers,
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes());
                }
            }
        });
        let resp = fetch_url_with_retry(
            &format!("http://{}/", addr),
            None,
            None,
            10,
            2,
            &HttpMethod::Get,
            None,
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert!(resp.status().is_success());
        assert_eq!(count.load(Ordering::SeqCst), 2, "429 must be retried");
    }

    #[tokio::test]
    async fn four_oh_eight_is_retried() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let count_srv = count.clone();
        std::thread::spawn(move || {
            for _ in 0..2 {
                if let Ok((stream, _)) = listener.accept() {
                    count_srv.fetch_add(1, Ordering::SeqCst);
                    let mut stream = stream;
                    let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                    let mut drain = String::new();
                    loop {
                        drain.clear();
                        if reader.read_line(&mut drain).unwrap() == 0 || drain == "\r\n" {
                            break;
                        }
                    }
                    let (status, body) = if count_srv.load(Ordering::SeqCst) == 1 {
                        ("408 Request Timeout", "timeout")
                    } else {
                        ("200 OK", "[{\"ok\":true}]")
                    };
                    let resp = format!(
                        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        status,
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes());
                }
            }
        });
        let resp = fetch_url_with_retry(
            &format!("http://{}/", addr),
            None,
            None,
            10,
            2,
            &HttpMethod::Get,
            None,
            &HashMap::new(),
        )
        .await
        .unwrap();
        assert!(resp.status().is_success());
        assert_eq!(count.load(Ordering::SeqCst), 2, "408 must be retried");
    }

    #[test]
    fn test_get_client_caches_same_params() {
        let ua = format!("TestClient/{}", uuid::Uuid::new_v4());
        let _c1 = get_client(None, Some(&ua), 10);
        let _c2 = get_client(None, Some(&ua), 10);
        let ours = CLIENT_POOL
            .iter()
            .filter(|e| e.key().contains(&ua))
            .count();
        assert_eq!(ours, 1, "same params should reuse one pool entry");
    }
}
