//! Prometheus metrics, hand-rolled on atomics (no registry crate): fixed
//! families, plus per-route HTTP series keyed by the matched route template.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

const BUCKETS: [f64; 12] = [0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 2.5];

#[derive(Default)]
pub struct Histogram {
    buckets: [AtomicU64; BUCKETS.len()],
    count: AtomicU64,
    /// Sum in microseconds.
    sum_us: AtomicU64,
}

impl Histogram {
    pub fn observe(&self, d: Duration) {
        let s = d.as_secs_f64();
        for (i, b) in BUCKETS.iter().enumerate() {
            if s <= *b {
                self.buckets[i].fetch_add(1, Relaxed);
            }
        }
        self.count.fetch_add(1, Relaxed);
        self.sum_us.fetch_add(d.as_micros() as u64, Relaxed);
    }

    fn render(&self, out: &mut String, name: &str, labels: &str) {
        let sep = if labels.is_empty() { "" } else { "," };
        for (i, b) in BUCKETS.iter().enumerate() {
            let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"{b}\"}} {}", self.buckets[i].load(Relaxed));
        }
        let c = self.count.load(Relaxed);
        let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {c}");
        let lb = if labels.is_empty() { String::new() } else { format!("{{{labels}}}") };
        let _ = writeln!(out, "{name}_sum{lb} {}", self.sum_us.load(Relaxed) as f64 / 1e6);
        let _ = writeln!(out, "{name}_count{lb} {c}");
    }
}

#[derive(Default)]
struct Route {
    by_class: [AtomicU64; 5], // 1xx..5xx
    latency: Histogram,
}

#[derive(Default)]
pub struct Metrics {
    pub head: AtomicU64,
    pub tip: AtomicU64,
    pub blocks_indexed: AtomicU64,
    pub txs_indexed: AtomicU64,
    pub commit: Histogram,
    pub rpc_ok: AtomicU64,
    pub rpc_err: AtomicU64,
    pub rpc_latency: Histogram,
    pub reorgs: AtomicU64,
    pub rolled_back: AtomicU64,
    pub ws_clients: AtomicU64,
    pub ws_dropped_slow: AtomicU64,
    pub rate_limited: AtomicU64,
    routes: Mutex<HashMap<String, &'static Route>>,
}

pub fn get() -> &'static Metrics {
    static M: OnceLock<Metrics> = OnceLock::new();
    M.get_or_init(Metrics::default)
}

impl Metrics {
    /// Records one HTTP request. Route entries are leaked once per route
    /// template (a fixed, small set), so the hot path is a short map lookup.
    pub fn http(&self, route: &str, status: u16, d: Duration) {
        let r = {
            let mut m = self.routes.lock().unwrap();
            match m.get(route) {
                Some(r) => *r,
                None => {
                    let r: &'static Route = Box::leak(Box::default());
                    m.insert(route.to_string(), r);
                    r
                }
            }
        };
        let class = (status / 100).clamp(1, 5) as usize - 1;
        r.by_class[class].fetch_add(1, Relaxed);
        r.latency.observe(d);
    }

    pub fn render(&self) -> String {
        let mut o = String::with_capacity(8192);
        let g = |o: &mut String, name: &str, help: &str, kind: &str, v: u64| {
            let _ = writeln!(o, "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {v}");
        };
        let head = self.head.load(Relaxed);
        let tip = self.tip.load(Relaxed);
        g(&mut o, "faster_head_block", "Highest block in the store", "gauge", head);
        g(&mut o, "faster_node_tip_block", "Node's latest block", "gauge", tip);
        g(&mut o, "faster_lag_blocks", "Blocks the store is behind the node", "gauge", tip.saturating_sub(head));
        g(&mut o, "faster_blocks_indexed_total", "Blocks committed", "counter", self.blocks_indexed.load(Relaxed));
        g(&mut o, "faster_txs_indexed_total", "Transactions committed", "counter", self.txs_indexed.load(Relaxed));
        g(&mut o, "faster_reorgs_total", "Chain reorganisations handled", "counter", self.reorgs.load(Relaxed));
        g(
            &mut o,
            "faster_rolled_back_blocks_total",
            "Blocks rolled back by reorgs",
            "counter",
            self.rolled_back.load(Relaxed),
        );
        g(&mut o, "faster_ws_clients", "Connected WebSocket clients", "gauge", self.ws_clients.load(Relaxed));
        g(
            &mut o,
            "faster_ws_dropped_slow_total",
            "WebSocket clients dropped for falling behind",
            "counter",
            self.ws_dropped_slow.load(Relaxed),
        );
        g(
            &mut o,
            "faster_rate_limited_total",
            "Requests rejected by the rate limiter",
            "counter",
            self.rate_limited.load(Relaxed),
        );
        let _ = writeln!(
            o,
            "# HELP faster_rpc_requests_total Node JSON-RPC requests\n# TYPE faster_rpc_requests_total counter"
        );
        let _ = writeln!(o, "faster_rpc_requests_total{{result=\"ok\"}} {}", self.rpc_ok.load(Relaxed));
        let _ = writeln!(o, "faster_rpc_requests_total{{result=\"error\"}} {}", self.rpc_err.load(Relaxed));
        let _ =
            writeln!(o, "# HELP faster_rpc_seconds Node JSON-RPC request latency\n# TYPE faster_rpc_seconds histogram");
        self.rpc_latency.render(&mut o, "faster_rpc_seconds", "");
        let _ =
            writeln!(o, "# HELP faster_commit_seconds Store commit latency\n# TYPE faster_commit_seconds histogram");
        self.commit.render(&mut o, "faster_commit_seconds", "");
        let routes: Vec<(String, &'static Route)> =
            self.routes.lock().unwrap().iter().map(|(k, v)| (k.clone(), *v)).collect();
        let _ =
            writeln!(o, "# HELP faster_http_requests_total HTTP requests\n# TYPE faster_http_requests_total counter");
        for (route, r) in &routes {
            for (i, c) in r.by_class.iter().enumerate() {
                let v = c.load(Relaxed);
                if v > 0 {
                    let _ = writeln!(o, "faster_http_requests_total{{route=\"{route}\",code=\"{}xx\"}} {v}", i + 1);
                }
            }
        }
        let _ = writeln!(o, "# HELP faster_http_seconds HTTP request latency\n# TYPE faster_http_seconds histogram");
        for (route, r) in &routes {
            r.latency.render(&mut o, "faster_http_seconds", &format!("route=\"{route}\""));
        }
        o
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_prometheus_text() {
        let m = Metrics::default();
        m.http("/api/blocks/{id}", 200, Duration::from_micros(300));
        m.http("/api/blocks/{id}", 404, Duration::from_millis(3));
        m.head.store(5, Relaxed);
        m.tip.store(9, Relaxed);
        let s = m.render();
        assert!(s.contains("faster_lag_blocks 4"));
        assert!(s.contains("faster_http_requests_total{route=\"/api/blocks/{id}\",code=\"2xx\"} 1"));
        assert!(s.contains("faster_http_requests_total{route=\"/api/blocks/{id}\",code=\"4xx\"} 1"));
        assert!(s.contains("faster_http_seconds_bucket{route=\"/api/blocks/{id}\",le=\"0.0005\"} 1"));
        assert!(s.contains("faster_http_seconds_count{route=\"/api/blocks/{id}\"} 2"));
    }
}
