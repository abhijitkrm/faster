//! Forwarding of calls faster-rpc does not answer itself (state, gas,
//! sending transactions) to an upstream node.

use std::time::Duration;

use serde_json::{json, Value};

pub struct Upstream {
    url: String,
    http: reqwest::Client,
}

impl Upstream {
    pub fn new(url: &str) -> Upstream {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(64)
            .build()
            .expect("http client");
        Upstream { url: url.to_string(), http }
    }

    /// Sends one call and returns the node's `result` or `error` object.
    pub async fn call(&self, method: &str, params: &Value) -> Result<Value, Value> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let r = match self.http.post(&self.url).json(&body).send().await {
            Ok(r) => r,
            Err(e) => return Err(json!({"code": -32603, "message": format!("upstream: {e}")})),
        };
        let v: Value = match r.json().await {
            Ok(v) => v,
            Err(e) => return Err(json!({"code": -32603, "message": format!("upstream response: {e}")})),
        };
        if let Some(e) = v.get("error") {
            return Err(e.clone());
        }
        Ok(v.get("result").cloned().unwrap_or(Value::Null))
    }
}
