use anyhow::Result;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use reqwest::{
    header::{HeaderMap, HeaderValue, USER_AGENT},
    Client, ClientBuilder, Proxy, Response,
};
use std::time::Duration;

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
        let proxy = Proxy::all(proxy_url)?;
        if proxy_url.starts_with("socks") {
            builder = builder.proxy(proxy);
        } else {
            builder = builder.proxy(proxy);
        }
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
) -> Result<Response> {
    let client = get_client(proxy, user_agent, timeout_secs);
    let response = client.get(url).send().await?;
    Ok(response)
}

pub async fn fetch_url_with_retry(
    url: &str,
    proxy: Option<&str>,
    user_agent: Option<&str>,
    timeout_secs: u64,
    max_retries: u32,
) -> Result<Response> {
    let mut last_error = None;
    for attempt in 0..=max_retries {
        match fetch_url(url, proxy, user_agent, timeout_secs).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    return Ok(resp);
                }
                last_error = Some(anyhow::anyhow!(
                    "HTTP {} for {}",
                    resp.status(),
                    url
                ));
            }
            Err(e) => {
                last_error = Some(e);
            }
        }
        if attempt < max_retries {
            let delay = Duration::from_millis(500 * 2u64.pow(attempt));
            tokio::time::sleep(delay).await;
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Request failed after {} retries", max_retries)))
}
