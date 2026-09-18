//! Block ingestion over the node's HTTP RPC — `GET /status` for the tip and
//! `GET /blocks?from=&to=` for the blocks — instead of the P2P wire.
//!
//! The P2P path decodes the node's bincode wire, which carries no version
//! tag: a Retracer built against a different node revision decodes garbage
//! without an error, and the pin needed six re-pins in eleven days. JSON
//! fails loudly on a shape change, the node's `/status` reports the version
//! its block JSON follows, and `B` is a plain serde struct — no Arxium crate
//! in the loop at all.
//!
//! Same contract as [`crate::run`]: ascending blocks on `block_tx`, a rewind
//! height on `rewind_rx` restarts from there, and every `/status` answer is
//! published as a [`NetworkView`].

use crate::{HasHeight, NetworkView};
use anyhow::{Context, Result};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{info, warn};

/// Oldest node whose block JSON this reader understands: `hash` on the block
/// and `payload_json` on each action arrived in `xc-rpc` 0.2.0. A node that
/// reports no version at all predates the field and is refused the same way.
pub const MIN_NODE_VERSION: (u64, u64, u64) = (0, 2, 0);

/// The node's `/blocks` page cap (`xc_storage::MAX_PAGE_SIZE`). Asking for
/// more is not an error — the node truncates — but each page would then be
/// one request for fewer blocks than it could carry.
const PAGE_SIZE: u64 = 100;

/// Matches the P2P path's `STATUS_POLL_INTERVAL`, for the same reason: the
/// tip we report is never more than one interval stale.
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const ERROR_BACKOFF: Duration = Duration::from_secs(2);

pub struct RpcConfig {
    pub node_rpc_url: String,
    pub node_rpc_token: Option<String>,
    /// Height to resume ingestion from; `None` starts at genesis.
    pub resume_from: Option<u64>,
}

#[derive(serde::Deserialize)]
struct Status {
    version: Option<String>,
    tip_height: u64,
    finalized_height: Option<u64>,
}

struct Client {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
}

impl Client {
    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let mut request = self.http.get(format!("{}{path}", self.base));
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        request
            .send()
            .await
            .with_context(|| format!("GET {path}"))?
            .error_for_status()
            .with_context(|| format!("GET {path}"))?
            .json()
            .await
            .with_context(|| format!("GET {path}: decoding body"))
    }
}

fn parse_version(v: &str) -> Option<(u64, u64, u64)> {
    let mut parts = v.trim_start_matches('v').split('.').map(|p| p.parse().ok());
    Some((parts.next()??, parts.next()??, parts.next()??))
}

/// Refuses a node whose block JSON predates what this reader expects.
fn check_version(status: &Status) -> Result<()> {
    let reported = status.version.as_deref().unwrap_or("<none>");
    let parsed = status.version.as_deref().and_then(parse_version);
    anyhow::ensure!(
        parsed.is_some_and(|v| v >= MIN_NODE_VERSION),
        "node reports version {reported}; this retracer needs at least {}.{}.{} \
         (block JSON with `hash` and `payload_json`)",
        MIN_NODE_VERSION.0,
        MIN_NODE_VERSION.1,
        MIN_NODE_VERSION.2
    );
    Ok(())
}

pub async fn run<B>(
    config: RpcConfig,
    block_tx: Sender<B>,
    mut rewind_rx: Receiver<u64>,
    network_tx: tokio::sync::watch::Sender<NetworkView>,
) -> Result<()>
where
    B: HasHeight + serde::de::DeserializeOwned + Send + 'static,
{
    let client = Client {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("building http client")?,
        base: config.node_rpc_url.trim_end_matches('/').to_owned(),
        token: config.node_rpc_token,
    };
    let mut next = config.resume_from.unwrap_or(0);
    let mut version_checked = false;
    info!(node = %client.base, from = next, "ingesting over rpc");

    loop {
        // A rewind from the indexer (fork detected) wins over whatever page
        // we were about to ask for.
        while let Ok(height) = rewind_rx.try_recv() {
            warn!(height, "rewinding rpc cursor");
            next = height;
        }

        let status: Status = match client.get("/status").await {
            Ok(status) => status,
            Err(err) => {
                warn!("node status unavailable: {err:#}");
                tokio::time::sleep(ERROR_BACKOFF).await;
                continue;
            }
        };
        if !version_checked {
            check_version(&status)?;
            version_checked = true;
        }
        let _ = network_tx.send(NetworkView {
            active_peer_count: 1,
            status_peer_count: 1,
            tip_height: Some(status.tip_height),
            finalized_height: status.finalized_height,
            last_status_at: Some(Instant::now()),
        });

        if next > status.tip_height {
            // Caught up: wait for the next block or a rewind, whichever first.
            tokio::select! {
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
                rewind = rewind_rx.recv() => match rewind {
                    Some(height) => { warn!(height, "rewinding rpc cursor"); next = height; }
                    None => return Ok(()),
                },
            }
            continue;
        }

        let to = status.tip_height.min(next + PAGE_SIZE - 1);
        let page: Vec<B> = match client.get(&format!("/blocks?from={next}&to={to}")).await {
            Ok(page) => page,
            Err(err) => {
                warn!(from = next, to, "block page unavailable: {err:#}");
                tokio::time::sleep(ERROR_BACKOFF).await;
                continue;
            }
        };
        anyhow::ensure!(
            !page.is_empty(),
            "node returned no blocks for {next}..={to}"
        );
        for block in page {
            let height = block.height();
            anyhow::ensure!(
                height == next,
                "node returned height {height}, expected {next}"
            );
            if block_tx.send(block).await.is_err() {
                return Ok(()); // indexer gone; shutting down
            }
            next = height + 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(serde::Deserialize)]
    struct TestBlock {
        height: u64,
    }
    impl HasHeight for TestBlock {
        fn height(&self) -> u64 {
            self.height
        }
    }

    /// Just enough HTTP to answer `/status` and `/blocks?from=&to=` for a
    /// chain of `tip + 1` blocks. Records every path it was asked for.
    async fn fake_node(tip: u64) -> (String, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let log = seen.clone();
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.unwrap();
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let path = req.split_whitespace().nth(1).unwrap_or("").to_string();
                log.lock().unwrap().push(path.clone());
                let body = if path == "/status" {
                    format!(r#"{{"version":"0.2.0","tip_height":{tip},"finalized_height":null}}"#)
                } else {
                    let q: std::collections::HashMap<_, _> = path
                        .split_once('?')
                        .map(|(_, q)| q.split('&').filter_map(|kv| kv.split_once('=')).collect())
                        .unwrap_or_default();
                    let from: u64 = q["from"].parse().unwrap();
                    let to: u64 = q["to"].parse::<u64>().unwrap().min(tip);
                    let blocks: Vec<String> = (from..=to)
                        .map(|h| format!(r#"{{"height":{h}}}"#))
                        .collect();
                    format!("[{}]", blocks.join(","))
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                sock.write_all(resp.as_bytes()).await.unwrap();
            }
        });
        (url, seen)
    }

    #[tokio::test]
    async fn pages_through_the_chain_then_polls_and_honours_a_rewind() {
        let (url, seen) = fake_node(149).await;
        let (block_tx, mut block_rx) = tokio::sync::mpsc::channel::<TestBlock>(1000);
        let (rewind_tx, rewind_rx) = tokio::sync::mpsc::channel::<u64>(4);
        let (network_tx, network_rx) = tokio::sync::watch::channel(NetworkView::default());
        let task = tokio::spawn(run(
            RpcConfig {
                node_rpc_url: format!("{url}/"),
                node_rpc_token: None,
                resume_from: Some(10),
            },
            block_tx,
            rewind_rx,
            network_tx,
        ));

        // 10..=149, in order, across two pages (10..=109, 110..=149).
        let mut got = Vec::new();
        while got.len() < 140 {
            got.push(block_rx.recv().await.unwrap().height);
        }
        assert_eq!(got, (10..=149).collect::<Vec<_>>());
        assert_eq!(network_rx.borrow().tip_height, Some(149));
        assert!(network_rx.borrow().has_fresh_status());
        assert!(
            seen.lock()
                .unwrap()
                .contains(&"/blocks?from=10&to=109".to_string())
        );

        // Caught up: a fork at 140 makes the indexer ask for 140 again.
        rewind_tx.send(140).await.unwrap();
        let refetched = block_rx.recv().await.unwrap().height;
        assert_eq!(refetched, 140);
        task.abort();
    }

    #[test]
    fn version_gate() {
        let status = |v: Option<&str>| Status {
            version: v.map(str::to_owned),
            tip_height: 0,
            finalized_height: None,
        };
        assert!(check_version(&status(None)).is_err());
        assert!(check_version(&status(Some("0.1.0"))).is_err());
        assert!(check_version(&status(Some("garbage"))).is_err());
        assert!(check_version(&status(Some("0.2.0"))).is_ok());
        assert!(check_version(&status(Some("v1.0.3"))).is_ok());
    }
}
