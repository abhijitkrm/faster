// Same dataset shape and workloads as the Go harness (dbbench), for Rust-side
// engines: RocksDB, redb, QMDB. QMDB is hash-indexed (no ordered iteration),
// so only ingest and point lookups apply to it.
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

const BATCH_BLK: u64 = 1000;

struct Cfg {
    blocks: u64,
    txper: u32,
    addrs: u64,
    payload: usize,
    ops: usize,
}

#[derive(Clone)]
struct Tx {
    block: u64,
    idx: u32,
    hash: [u8; 32],
    from: u32,
    to: u32,
}

fn addr(i: u32) -> [u8; 20] {
    let mut b = [0u8; 20];
    b[0] = 0xaa;
    b[16..].copy_from_slice(&i.to_be_bytes());
    b
}
fn tx_key(b: u64, i: u32) -> [u8; 12] {
    let mut k = [0u8; 12];
    k[..8].copy_from_slice(&b.to_be_bytes());
    k[8..].copy_from_slice(&i.to_be_bytes());
    k
}
fn inv_key(b: u64, i: u32) -> [u8; 12] {
    tx_key(!b, !i)
}
fn hash_of(b: u64, i: u32) -> [u8; 32] {
    Sha256::digest(tx_key(b, i)).into()
}

fn gen(c: &Cfg, mut f: impl FnMut(&[Tx])) {
    let mut r = rand::rngs::StdRng::seed_from_u64(1);
    let z = Zipf::new(c.addrs, 1.2).unwrap();
    let mut batch = Vec::with_capacity((BATCH_BLK * c.txper as u64) as usize);
    for b in 0..c.blocks {
        for i in 0..c.txper {
            let from = z.sample(&mut r) as u32 - 1;
            let to = z.sample(&mut r) as u32 - 1;
            batch.push(Tx { block: b, idx: i, hash: hash_of(b, i), from, to });
        }
        if (b + 1) % BATCH_BLK == 0 || b == c.blocks - 1 {
            f(&batch);
            batch.clear();
        }
    }
}

fn value(t: &Tx, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(40 + payload.len());
    v.extend_from_slice(&addr(t.from));
    v.extend_from_slice(&addr(t.to));
    v.extend_from_slice(payload);
    v
}

trait Engine: Send + Sync {
    fn name(&self) -> &'static str;
    fn load(&self, batch: &[Tx], payload: &[u8]);
    fn finish_load(&self) {}
    fn get_tx(&self, hash: &[u8; 32]) -> bool;
    fn addr_page(&self, _a: &[u8; 20], _before: Option<[u8; 12]>, _n: usize) -> Option<usize> {
        None
    }
    fn latest_blocks(&self, _n: usize) -> Option<usize> {
        None
    }
    fn scan_to(&self, _a: &[u8; 20], _from: u64, _to: u64) -> Option<usize> {
        None
    }
    fn agg(&self) -> Option<usize> {
        None
    }
    fn size(&self) -> u64;
}

fn dir_size(p: &Path) -> u64 {
    let mut s = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let m = e.metadata().unwrap();
            s += if m.is_dir() { dir_size(&e.path()) } else { m.len() };
        }
    }
    s
}

// ---------------- RocksDB ----------------
struct Rocks {
    db: rocksdb::DB,
    path: String,
}
impl Rocks {
    fn new(path: &str) -> Self {
        let _ = std::fs::remove_dir_all(path);
        let mut o = rocksdb::Options::default();
        o.create_if_missing(true);
        o.increase_parallelism(8);
        o.set_write_buffer_size(256 << 20);
        let mut bo = rocksdb::BlockBasedOptions::default();
        bo.set_block_cache(&rocksdb::Cache::new_lru_cache(4 << 30));
        bo.set_bloom_filter(10.0, false);
        bo.set_cache_index_and_filter_blocks(true);
        bo.set_pin_l0_filter_and_index_blocks_in_cache(true);
        o.set_block_based_table_factory(&bo);
        o.set_compression_type(rocksdb::DBCompressionType::Lz4);
        Rocks { db: rocksdb::DB::open(&o, path).unwrap(), path: path.into() }
    }
}
fn pfx(p: u8, k: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + k.len());
    v.push(p);
    v.extend_from_slice(k);
    v
}
impl Engine for Rocks {
    fn name(&self) -> &'static str {
        "rocksdb"
    }
    fn load(&self, batch: &[Tx], payload: &[u8]) {
        let mut wb = rocksdb::WriteBatch::default();
        let mut last = u64::MAX;
        for t in batch {
            let k = tx_key(t.block, t.idx);
            wb.put(pfx(b't', &k), value(t, payload));
            wb.put(pfx(b'h', &t.hash), k);
            let ik = inv_key(t.block, t.idx);
            let mut ak = pfx(b'a', &addr(t.from));
            ak.extend_from_slice(&ik);
            wb.put(ak, []);
            if t.to != t.from {
                let mut ak = pfx(b'a', &addr(t.to));
                ak.extend_from_slice(&ik);
                wb.put(ak, []);
            }
            if t.block != last {
                wb.put(pfx(b'b', &t.block.to_be_bytes()), &payload[..120]);
                last = t.block;
            }
        }
        let mut wo = rocksdb::WriteOptions::default();
        wo.set_sync(true);
        self.db.write_opt(wb, &wo).unwrap();
    }
    fn get_tx(&self, hash: &[u8; 32]) -> bool {
        let k = self.db.get_pinned(pfx(b'h', hash)).unwrap().unwrap();
        self.db.get_pinned(pfx(b't', &k)).unwrap().is_some()
    }
    fn addr_page(&self, a: &[u8; 20], before: Option<[u8; 12]>, n: usize) -> Option<usize> {
        let lo = pfx(b'a', a);
        let mut hi = lo.clone();
        hi.extend_from_slice(&[0xff; 13]);
        let start = match before {
            None => lo.clone(),
            Some(b) => {
                let mut s = lo.clone();
                s.extend_from_slice(&inv_key(u64::from_be_bytes(b[..8].try_into().unwrap()), u32::from_be_bytes(b[8..].try_into().unwrap())));
                s.push(0);
                s
            }
        };
        let mut ro = rocksdb::ReadOptions::default();
        ro.set_iterate_upper_bound(hi);
        let it = self.db.iterator_opt(rocksdb::IteratorMode::From(&start, rocksdb::Direction::Forward), ro);
        let mut cnt = 0;
        for kv in it {
            if cnt >= n {
                break;
            }
            let (k, _) = kv.unwrap();
            let ik = &k[21..];
            let tk = tx_key(!u64::from_be_bytes(ik[..8].try_into().unwrap()), !u32::from_be_bytes(ik[8..].try_into().unwrap()));
            self.db.get_pinned(pfx(b't', &tk)).unwrap().unwrap();
            cnt += 1;
        }
        Some(cnt)
    }
    fn latest_blocks(&self, n: usize) -> Option<usize> {
        let mut ro = rocksdb::ReadOptions::default();
        ro.set_iterate_lower_bound(vec![b'b']);
        let it = self.db.iterator_opt(rocksdb::IteratorMode::From(&[b'c'], rocksdb::Direction::Reverse), ro);
        Some(it.take(n).count())
    }
    fn scan_to(&self, a: &[u8; 20], from: u64, to: u64) -> Option<usize> {
        let mut ro = rocksdb::ReadOptions::default();
        ro.set_iterate_upper_bound(pfx(b't', &tx_key(to, 0)));
        let start = pfx(b't', &tx_key(from, 0));
        let it = self.db.iterator_opt(rocksdb::IteratorMode::From(&start, rocksdb::Direction::Forward), ro);
        Some(it.filter(|kv| &kv.as_ref().unwrap().1[20..40] == a).count())
    }
    fn agg(&self) -> Option<usize> {
        let mut ro = rocksdb::ReadOptions::default();
        ro.set_iterate_upper_bound(vec![b'u']);
        let it = self.db.iterator_opt(rocksdb::IteratorMode::From(&[b't'], rocksdb::Direction::Forward), ro);
        let mut m: HashMap<u64, usize> = HashMap::new();
        for kv in it {
            let (k, _) = kv.unwrap();
            *m.entry(u64::from_be_bytes(k[1..9].try_into().unwrap()) / 10000).or_default() += 1;
        }
        Some(m.len())
    }
    fn size(&self) -> u64 {
        dir_size(Path::new(&self.path))
    }
}

// ---------------- redb ----------------
use redb::{MultimapTableDefinition, ReadableTable, TableDefinition};
const R_TX: TableDefinition<&[u8; 12], &[u8]> = TableDefinition::new("txs");
const R_HASH: TableDefinition<&[u8; 32], &[u8; 12]> = TableDefinition::new("hashes");
const R_ADDR: MultimapTableDefinition<&[u8; 20], &[u8; 12]> = MultimapTableDefinition::new("addr");
const R_BLK: TableDefinition<u64, &[u8]> = TableDefinition::new("blocks");
struct Redb {
    db: redb::Database,
    path: String,
}
impl Redb {
    fn new(path: &str) -> Self {
        let _ = std::fs::remove_file(path);
        let db = redb::Builder::new().set_cache_size(4 << 30).create(path).unwrap();
        Redb { db, path: path.into() }
    }
}
impl Engine for Redb {
    fn name(&self) -> &'static str {
        "redb"
    }
    fn load(&self, batch: &[Tx], payload: &[u8]) {
        let w = self.db.begin_write().unwrap();
        {
            let mut tx = w.open_table(R_TX).unwrap();
            let mut h = w.open_table(R_HASH).unwrap();
            let mut a = w.open_multimap_table(R_ADDR).unwrap();
            let mut bl = w.open_table(R_BLK).unwrap();
            let mut last = u64::MAX;
            for t in batch {
                let k = tx_key(t.block, t.idx);
                tx.insert(&k, value(t, payload).as_slice()).unwrap();
                h.insert(&t.hash, &k).unwrap();
                let ik = inv_key(t.block, t.idx);
                a.insert(&addr(t.from), &ik).unwrap();
                if t.to != t.from {
                    a.insert(&addr(t.to), &ik).unwrap();
                }
                if t.block != last {
                    bl.insert(t.block, &payload[..120]).unwrap();
                    last = t.block;
                }
            }
        }
        w.commit().unwrap();
    }
    fn get_tx(&self, hash: &[u8; 32]) -> bool {
        let r = self.db.begin_read().unwrap();
        let k = *r.open_table(R_HASH).unwrap().get(hash).unwrap().unwrap().value();
        r.open_table(R_TX).unwrap().get(&k).unwrap().is_some()
    }
    fn addr_page(&self, a: &[u8; 20], before: Option<[u8; 12]>, n: usize) -> Option<usize> {
        let r = self.db.begin_read().unwrap();
        let am = r.open_multimap_table(R_ADDR).unwrap();
        let txs = r.open_table(R_TX).unwrap();
        let mut cnt = 0;
        let vals = am.get(a).unwrap();
        if before.is_some() {
            return None; // multimap values can't be seeked; a composite key would be used in practice
        }
        let skip_to: Option<[u8; 12]> = None;
        for v in vals {
            let ik = *v.unwrap().value();
            if let Some(s) = skip_to {
                if ik <= s {
                    continue; // redb multimap has no seek-within-values; deep pages walk
                }
            }
            if cnt >= n {
                break;
            }
            let tk = tx_key(!u64::from_be_bytes(ik[..8].try_into().unwrap()), !u32::from_be_bytes(ik[8..].try_into().unwrap()));
            txs.get(&tk).unwrap().unwrap();
            cnt += 1;
        }
        Some(cnt)
    }
    fn latest_blocks(&self, n: usize) -> Option<usize> {
        let r = self.db.begin_read().unwrap();
        let t = r.open_table(R_BLK).unwrap();
        Some(t.iter().unwrap().rev().take(n).count())
    }
    fn scan_to(&self, a: &[u8; 20], from: u64, to: u64) -> Option<usize> {
        let r = self.db.begin_read().unwrap();
        let t = r.open_table(R_TX).unwrap();
        let (lo, hi) = (tx_key(from, 0), tx_key(to, 0));
        Some(t.range::<&[u8; 12]>(&lo..&hi).unwrap().filter(|kv| &kv.as_ref().unwrap().1.value()[20..40] == a).count())
    }
    fn agg(&self) -> Option<usize> {
        let r = self.db.begin_read().unwrap();
        let t = r.open_table(R_TX).unwrap();
        let mut m: HashMap<u64, usize> = HashMap::new();
        for kv in t.iter().unwrap() {
            let k = *kv.unwrap().0.value();
            *m.entry(u64::from_be_bytes(k[..8].try_into().unwrap()) / 10000).or_default() += 1;
        }
        Some(m.len())
    }
    fn size(&self) -> u64 {
        std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0)
    }
}


// ---------------- Fjall ----------------
struct Fjall {
    ks: fjall::Keyspace,
    txs: fjall::PartitionHandle,
    hashes: fjall::PartitionHandle,
    addrs: fjall::PartitionHandle,
    blocks: fjall::PartitionHandle,
    path: String,
}
impl Fjall {
    fn new(path: &str) -> Self {
        let _ = std::fs::remove_dir_all(path);
        let ks = fjall::Config::new(path).cache_size(4 << 30).open().unwrap();
        let o = || fjall::PartitionCreateOptions::default();
        Fjall {
            txs: ks.open_partition("txs", o()).unwrap(),
            hashes: ks.open_partition("hashes", o().bloom_filter_bits(Some(10))).unwrap(),
            addrs: ks.open_partition("addrs", o()).unwrap(),
            blocks: ks.open_partition("blocks", o()).unwrap(),
            ks,
            path: path.into(),
        }
    }
}
impl Engine for Fjall {
    fn name(&self) -> &'static str {
        "fjall"
    }
    fn load(&self, batch: &[Tx], payload: &[u8]) {
        let mut b = self.ks.batch();
        let mut last = u64::MAX;
        for t in batch {
            let k = tx_key(t.block, t.idx);
            b.insert(&self.txs, k, value(t, payload));
            b.insert(&self.hashes, t.hash, k);
            let ik = inv_key(t.block, t.idx);
            let mut ak = addr(t.from).to_vec();
            ak.extend_from_slice(&ik);
            b.insert(&self.addrs, ak, []);
            if t.to != t.from {
                let mut ak = addr(t.to).to_vec();
                ak.extend_from_slice(&ik);
                b.insert(&self.addrs, ak, []);
            }
            if t.block != last {
                b.insert(&self.blocks, t.block.to_be_bytes(), &payload[..120]);
                last = t.block;
            }
        }
        b.commit().unwrap();
        self.ks.persist(fjall::PersistMode::SyncAll).unwrap();
    }
    fn get_tx(&self, hash: &[u8; 32]) -> bool {
        let k = self.hashes.get(hash).unwrap().unwrap();
        self.txs.get(&k).unwrap().is_some()
    }
    fn addr_page(&self, a: &[u8; 20], before: Option<[u8; 12]>, n: usize) -> Option<usize> {
        let lo = match before {
            None => a.to_vec(),
            Some(b) => {
                let mut s = a.to_vec();
                s.extend_from_slice(&inv_key(u64::from_be_bytes(b[..8].try_into().unwrap()), u32::from_be_bytes(b[8..].try_into().unwrap())));
                s.push(0);
                s
            }
        };
        let mut hi = a.to_vec();
        hi.extend_from_slice(&[0xff; 13]);
        let mut cnt = 0;
        for kv in self.addrs.range(lo..hi).take(n) {
            let (k, _) = kv.unwrap();
            let ik = &k[20..];
            let tk = tx_key(!u64::from_be_bytes(ik[..8].try_into().unwrap()), !u32::from_be_bytes(ik[8..].try_into().unwrap()));
            self.txs.get(tk).unwrap().unwrap();
            cnt += 1;
        }
        Some(cnt)
    }
    fn latest_blocks(&self, n: usize) -> Option<usize> {
        Some(self.blocks.iter().rev().take(n).count())
    }
    fn scan_to(&self, a: &[u8; 20], from: u64, to: u64) -> Option<usize> {
        Some(self.txs.range(tx_key(from, 0)..tx_key(to, 0)).filter(|kv| &kv.as_ref().unwrap().1[20..40] == a).count())
    }
    fn agg(&self) -> Option<usize> {
        let mut m: HashMap<u64, usize> = HashMap::new();
        for kv in self.txs.iter() {
            let (k, _) = kv.unwrap();
            *m.entry(u64::from_be_bytes(k[..8].try_into().unwrap()) / 10000).or_default() += 1;
        }
        Some(m.len())
    }
    fn size(&self) -> u64 {
        dir_size(Path::new(&self.path))
    }
}

// ---------------- QMDB ----------------
use parking_lot::{Mutex, RwLock};
use qmdb::config::Config as QConfig;
use qmdb::tasks::TasksManager;
use qmdb::test_helper::SimpleTask;
use qmdb::{AdsCore, AdsWrap, ADS};
use qmdb_common::def::OP_CREATE;
use qmdb_common::entryfile::entry::entry_key_hash;
use qmdb_common::entryfile::readbuf::ReadBuf;
use qmdb_common::task::taskid::join_task_id;
use qmdb_common::utils::changeset::ChangeSet;

struct Qmdb {
    ads: Mutex<AdsWrap<SimpleTask>>,
    shared: Mutex<Option<qmdb::SharedAdsWrap>>,
    height: Mutex<i64>,
    path: String,
}
// SharedAdsWrap is used read-only from many threads after loading.
unsafe impl Sync for Qmdb {}
unsafe impl Send for Qmdb {}
impl Qmdb {
    fn new(path: &str) -> Self {
        let _ = std::fs::remove_dir_all(path);
        let config = QConfig::from_dir(path);
        AdsCore::init_dir(&config);
        Qmdb { ads: Mutex::new(AdsWrap::new(&config)), shared: Mutex::new(None), height: Mutex::new(0), path: path.into() }
    }
}
impl Engine for Qmdb {
    fn name(&self) -> &'static str {
        "qmdb"
    }
    fn load(&self, batch: &[Tx], payload: &[u8]) {
        let mut h = self.height.lock();
        *h += 1;
        let height = *h;
        let mut tasks = Vec::new();
        for chunk in batch.chunks(1000) {
            let mut cset = ChangeSet::new();
            for t in chunk {
                let kh = entry_key_hash(&t.hash[..]);
                cset.add_op(OP_CREATE, &kh, &t.hash[..], &value(t, payload), None);
            }
            cset.sort();
            tasks.push(RwLock::new(Some(SimpleTask::new(vec![cset]))));
        }
        let n = tasks.len();
        let mut ads = self.ads.lock();
        ads.start_block(height, Arc::new(TasksManager::new(tasks)));
        let shared = ads.get_shared();
        shared.insert_extra_data(height, String::new());
        for i in 0..n {
            shared.add_task(join_task_id(height, i, i == n - 1));
        }
    }
    fn finish_load(&self) {
        let mut ads = self.ads.lock();
        ads.flush();
        *self.shared.lock() = Some(ads.get_shared());
    }
    fn get_tx(&self, hash: &[u8; 32]) -> bool {
        let kh = entry_key_hash(&hash[..]);
        let mut buf = ReadBuf::new();
        let s = self.shared.lock().as_ref().unwrap().clone();
        s.read_entry(-1, &kh[..], &[], &mut buf) > 0
    }
    fn size(&self) -> u64 {
        dir_size(Path::new(&self.path))
    }
}

// ---------------- harness ----------------
fn pct(v: &[Duration], p: f64) -> Duration {
    v[((v.len() - 1) as f64 * p) as usize]
}
fn measure(label: &str, ops: usize, mut f: impl FnMut(usize) -> Option<usize>) {
    let mut lat = Vec::with_capacity(ops);
    let st = Instant::now();
    for i in 0..ops {
        let t = Instant::now();
        if f(i).is_none() {
            println!("  {:<26} n/a (no ordered index in this engine)", label);
            return;
        }
        lat.push(t.elapsed());
    }
    let wall = st.elapsed();
    lat.sort();
    println!(
        "  {:<26} p50 {:>9.3?}  p99 {:>9.3?}  p99.9 {:>9.3?}  1-thread {:>8.0} ops/s",
        label,
        pct(&lat, 0.5),
        pct(&lat, 0.99),
        pct(&lat, 0.999),
        ops as f64 / wall.as_secs_f64()
    );
}
fn parallel(label: &str, e: &Arc<dyn Engine>, ops: usize, threads: usize, f: fn(&dyn Engine, usize) -> Option<usize>) {
    if f(e.as_ref(), 0).is_none() {
        println!("  {:<26} n/a", label);
        return;
    }
    let per = ops / threads;
    let st = Instant::now();
    let hs: Vec<_> = (0..threads)
        .map(|t| {
            let e = e.clone();
            std::thread::spawn(move || {
                for i in 0..per {
                    if f(e.as_ref(), t * per + i).is_none() {
                        return;
                    }
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    println!("  {:<26} {} threads {:>10.0} ops/s", label, threads, (per * threads) as f64 / st.elapsed().as_secs_f64());
}

static HASHES: std::sync::OnceLock<Vec<[u8; 32]>> = std::sync::OnceLock::new();
static WARM: std::sync::OnceLock<Vec<[u8; 20]>> = std::sync::OnceLock::new();

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |k: &str, d: &str| -> String {
        args.iter().position(|a| a == k).map(|i| args[i + 1].clone()).unwrap_or(d.into())
    };
    let c = Cfg {
        blocks: get("-blocks", "500000").parse().unwrap(),
        txper: get("-txper", "10").parse().unwrap(),
        addrs: get("-addrs", "1000000").parse().unwrap(),
        payload: 180,
        ops: get("-ops", "100000").parse().unwrap(),
    };
    let dir = get("-dir", "./data");
    let engines = get("-engines", "rocksdb,fjall,redb,qmdb");
    std::fs::create_dir_all(&dir).unwrap();
    let total = c.blocks * c.txper as u64;
    let mut r = rand::rngs::StdRng::seed_from_u64(7);
    let hashes: Vec<[u8; 32]> = (0..c.ops)
        .map(|_| {
            let n = r.gen_range(0..total);
            hash_of(n / c.txper as u64, (n % c.txper as u64) as u32)
        })
        .collect();
    let warm: Vec<[u8; 20]> = (0..c.ops).map(|_| addr(100 + r.gen_range(0..10000))).collect();
    HASHES.set(hashes.clone()).unwrap();
    WARM.set(warm.clone()).unwrap();
    let hot = addr(0);
    let mid = tx_key(c.blocks / 2, 0);
    let payload = vec![7u8; c.payload];

    for name in engines.split(',') {
        let e: Arc<dyn Engine> = match name {
            "rocksdb" => Arc::new(Rocks::new(&format!("{dir}/rocksdb"))),
            "redb" => Arc::new(Redb::new(&format!("{dir}/redb.db"))),
            "qmdb" => Arc::new(Qmdb::new(&format!("{dir}/qmdb"))),
            "fjall" => Arc::new(Fjall::new(&format!("{dir}/fjall"))),
            _ => panic!("unknown engine"),
        };
        println!("== {}: loading {} blocks / {} txs", e.name(), c.blocks, total);
        let st = Instant::now();
        gen(&c, |b| e.load(b, &payload));
        e.finish_load();
        let el = st.elapsed().as_secs_f64();
        println!("  load {:.1}s  = {:.0} tx/s ingest  size {:.2} GB", el, total as f64 / el, e.size() as f64 / 1e9);
        measure("tx by hash", c.ops, |i| e.get_tx(&hashes[i]).then_some(1));
        measure("latest 20 blocks", c.ops / 10, |_| e.latest_blocks(20));
        measure("addr page warm (25)", c.ops / 10, |i| e.addr_page(&warm[i], None, 25));
        measure("addr page hot (25)", c.ops / 10, |_| e.addr_page(&hot, None, 25));
        measure("addr page hot deep (25)", c.ops / 10, |_| e.addr_page(&hot, Some(mid), 25));
        measure("range scan to=X 100k blk", 20, |_| e.scan_to(&addr(5), 100000, 200000));
        measure("agg full history", 5, |_| e.agg());
        parallel("tx by hash", &e, c.ops * 4, 8, |e, i| {
            let h = HASHES.get().unwrap();
            e.get_tx(&h[i % h.len()]).then_some(1)
        });
        parallel("addr page warm (25)", &e, c.ops, 8, |e, i| {
            let w = WARM.get().unwrap();
            e.addr_page(&w[i % w.len()], None, 25)
        });
    }
}
