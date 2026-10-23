//! Counters and latency histograms, rendered in Prometheus text format.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const BOUNDS_MS: [f64; 14] = [0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 1000.0, 5000.0, 30000.0];

#[derive(Default)]
pub struct Hist {
    buckets: [AtomicU64; 14],
    count: AtomicU64,
    sum_us: AtomicU64,
}

impl Hist {
    pub fn observe(&self, d: Duration) {
        let ms = d.as_secs_f64() * 1000.0;
        for (i, b) in BOUNDS_MS.iter().enumerate() {
            if ms <= *b {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(d.as_micros() as u64, Ordering::Relaxed);
    }

    fn render(&self, name: &str, help: &str, out: &mut String) {
        let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} histogram");
        for (i, b) in BOUNDS_MS.iter().enumerate() {
            let _ = writeln!(out, "{name}_bucket{{le=\"{}\"}} {}", b / 1000.0, self.buckets[i].load(Ordering::Relaxed));
        }
        let c = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {c}");
        let _ = writeln!(out, "{name}_sum {}", self.sum_us.load(Ordering::Relaxed) as f64 / 1e6);
        let _ = writeln!(out, "{name}_count {c}");
    }
}

#[derive(Default)]
pub struct Metrics {
    pub blocks: AtomicU64,
    pub rows: AtomicU64,
    pub reorgs: AtomicU64,
    pub queries: AtomicU64,
    pub query_errors: AtomicU64,
    pub query_rows: AtomicU64,
    pub row_groups_read: AtomicU64,
    pub row_groups_skipped: AtomicU64,
    pub rate_limited: AtomicU64,
    pub decode: Hist,
    pub chunk_write: Hist,
    pub query: Hist,
}

impl Metrics {
    pub fn render(&self, gauges: &[(&str, &str, f64)]) -> String {
        let mut s = String::new();
        let counters: [(&str, &str, &AtomicU64); 9] = [
            ("fsync_ingested_blocks_total", "Blocks ingested", &self.blocks),
            ("fsync_ingested_rows_total", "Rows ingested (all tables)", &self.rows),
            ("fsync_reorgs_total", "Blocks dropped by reorgs", &self.reorgs),
            ("fsync_queries_total", "Queries answered", &self.queries),
            ("fsync_query_errors_total", "Queries rejected or failed", &self.query_errors),
            ("fsync_query_rows_total", "Rows returned", &self.query_rows),
            ("fsync_row_groups_read_total", "Row groups read by queries", &self.row_groups_read),
            ("fsync_row_groups_skipped_total", "Row groups skipped by filters", &self.row_groups_skipped),
            ("fsync_rate_limited_total", "Requests refused by the rate limiter", &self.rate_limited),
        ];
        for (n, h, c) in counters {
            let _ = writeln!(s, "# HELP {n} {h}\n# TYPE {n} counter\n{n} {}", c.load(Ordering::Relaxed));
        }
        for (n, h, v) in gauges {
            let _ = writeln!(s, "# HELP {n} {h}\n# TYPE {n} gauge\n{n} {v}");
        }
        self.decode.render("fsync_decode_seconds", "Decode time per fetched window", &mut s);
        self.chunk_write.render("fsync_chunk_write_seconds", "Chunk build and publish time", &mut s);
        self.query.render("fsync_query_seconds", "Query time", &mut s);
        s
    }
}
