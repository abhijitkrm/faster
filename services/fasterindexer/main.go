package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/gjson"
	"golang.org/x/time/rate"
)

type Config struct {
	RPCURL      string
	RedisURL    string
	DatabaseURL string
	BatchSize   int
	PollMs      int
	LogLevel    string
	StartBlock  int64
	RPS         float64
}

func loadConfig() Config {
	c := Config{
		RPCURL:      getEnv("RPC_URL", "http://localhost:8545"),
		RedisURL:    getEnv("REDIS_URL", "redis://localhost:6379"),
		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/explorer"),
		BatchSize:   getEnvInt("BATCH_SIZE", 100),
		PollMs:      getEnvInt("POLL_MS", 1000),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
		StartBlock:  getEnvInt64("START_BLOCK", 0),
		RPS:         getEnvFloat("RPS", 1),
	}
	return c
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func getEnvFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return n
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

type Indexer struct {
	cfg       Config
	rpc       *rpcClient
	redis     *redis.Client
	db        *pgxpool.Pool
	lastBlock int64
	stats     Stats
	mu        sync.Mutex
}

type Stats struct {
	BlocksProcessed      int64
	TransactionsProcessed int64
	LogsProcessed        int64
	Errors               int64
}

type rpcClient struct {
	url string
	c   *http.Client
	lim *rate.Limiter
}

func newRPCClient(url string, rps float64) *rpcClient {
	return &rpcClient{
		url: url,
		c:   &http.Client{Timeout: 10 * time.Second},
		lim: rate.NewLimiter(rate.Limit(rps), 1),
	}
}

func (r *rpcClient) call(ctx context.Context, method string, params []any) (gjson.Result, error) {
	if err := r.lim.Wait(ctx); err != nil {
		return gjson.Result{}, err
	}
	body := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	b, err := json.Marshal(body)
	if err != nil {
		return gjson.Result{}, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", r.url, strings.NewReader(string(b)))
	if err != nil {
		return gjson.Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.c.Do(req)
	if err != nil {
		return gjson.Result{}, err
	}
	defer resp.Body.Close()
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return gjson.Result{}, err
	}
	if out.Error != nil {
		return gjson.Result{}, fmt.Errorf("rpc %s: %s", method, out.Error.Message)
	}
	return gjson.ParseBytes(out.Result), nil
}

func main() {
	cfg := loadConfig()
	level := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("postgres connect", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		slog.Error("postgres ping", "err", err)
		os.Exit(1)
	}

	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		slog.Error("redis parse url", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("redis ping", "err", err)
		os.Exit(1)
	}

	lastBlock := int64(-1)
	if cfg.StartBlock > 0 {
		lastBlock = cfg.StartBlock - 1
	} else if v, err := rdb.Get(ctx, "fasterindexer:lastBlock").Result(); err == nil && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			lastBlock = n
		} else {
			slog.Warn("parse lastBlock", "val", v, "err", err)
		}
	} else if v, err := rdb.Get(ctx, "listener:lastBlock").Result(); err == nil && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			lastBlock = n
		}
	}

	idx := &Indexer{
		cfg:       cfg,
		rpc:       newRPCClient(cfg.RPCURL, cfg.RPS),
		redis:     rdb,
		db:        db,
		lastBlock: lastBlock,
	}

	if err := idx.ensureSchema(ctx); err != nil {
		slog.Error("schema ensure", "err", err)
		os.Exit(1)
	}

	go idx.startHealthServer()
	idx.run(ctx)
}

func (idx *Indexer) ensureSchema(ctx context.Context) error {
	_, err := idx.db.Exec(ctx, schemaSQL)
	return err
}

func (idx *Indexer) startHealthServer() {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		idx.mu.Lock()
		stats := idx.stats
		idx.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "ok",
			"stats":  stats,
		})
	})
	if err := http.ListenAndServe(":3102", nil); err != nil {
		slog.Error("health server", "err", err)
	}
}

func (idx *Indexer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		tip, err := idx.getBlockNumber(ctx)
		if err != nil {
			idx.incErrors()
			slog.Error("get block number", "err", err)
			time.Sleep(time.Duration(idx.cfg.PollMs) * time.Millisecond)
			continue
		}

		if idx.lastBlock == -1 {
			idx.lastBlock = tip - 1
		}

		for idx.lastBlock < tip {
			start := idx.lastBlock + 1
			end := min(tip, idx.lastBlock+int64(idx.cfg.BatchSize))
			if err := idx.indexRange(ctx, start, end); err != nil {
				idx.incErrors()
				slog.Error("index range", "from", start, "to", end, "err", err)
				time.Sleep(2 * time.Second)
				break
			}
			idx.lastBlock = end
			if err := idx.redis.Set(ctx, "fasterindexer:lastBlock", strconv.FormatInt(end, 10), 0).Err(); err != nil {
				slog.Warn("save lastBlock", "err", err)
			}
		}

		time.Sleep(time.Duration(idx.cfg.PollMs) * time.Millisecond)
	}
}

func (idx *Indexer) getBlockNumber(ctx context.Context) (int64, error) {
	res, err := idx.rpc.call(ctx, "eth_blockNumber", []any{})
	if err != nil {
		return 0, err
	}
	return parseHex(res.String()), nil
}

func (idx *Indexer) indexRange(ctx context.Context, from, to int64) error {
	nums := make([]int64, 0, to-from+1)
	for i := from; i <= to; i++ {
		nums = append(nums, i)
	}

	type result struct {
		block   gjson.Result
		receipts gjson.Result
		err     error
	}

	ch := make(chan result, len(nums))
	var wg sync.WaitGroup
	for _, n := range nums {
		wg.Add(1)
		go func(n int64) {
			defer wg.Done()
			hex := "0x" + strconv.FormatInt(n, 16)
			block, err1 := idx.rpc.call(ctx, "eth_getBlockByNumber", []any{hex, true})
			receipts, err2 := idx.rpc.call(ctx, "eth_getBlockReceipts", []any{hex})
			if err1 != nil {
				ch <- result{err: err1}
				return
			}
			if err2 != nil {
				ch <- result{block: block, receipts: gjson.Result{}, err: err2}
				return
			}
			ch <- result{block: block, receipts: receipts}
		}(n)
	}
	wg.Wait()
	close(ch)

	var (
		blocks       []Block
		transactions []Transaction
		logs         []Log
		addresses    []Address
	)

	addrCounts := make(map[string]int)

	for r := range ch {
		if r.err != nil {
			return r.err
		}
		b := idx.normalizeBlock(r.block)
		blocks = append(blocks, b)

		receiptMap := make(map[string]gjson.Result)
		r.receipts.ForEach(func(_, v gjson.Result) bool {
			receiptMap[strings.ToLower(v.Get("transactionHash").String())] = v
			return true
		})

		r.block.Get("transactions").ForEach(func(_, tx gjson.Result) bool {
			t := idx.normalizeTransaction(tx, b.Hash, b.Number, b.Timestamp, receiptMap)
			transactions = append(transactions, t)

			if t.From != "" {
				addrCounts[t.From]++
			}
			if t.To != nil && *t.To != "" {
				addrCounts[*t.To]++
			}

			receipt := receiptMap[t.Hash]
			receipt.Get("logs").ForEach(func(_, l gjson.Result) bool {
				log := idx.normalizeLog(l, t.Hash, b.Number, b.Hash)
				logs = append(logs, log)
				if log.Address != "" {
					addrCounts[log.Address]++
				}
				return true
			})
			return true
		})
	}

	for a, c := range addrCounts {
		addresses = append(addresses, Address{Address: a, Count: c})
	}

	if err := idx.writeBatch(ctx, blocks, transactions, logs, addresses); err != nil {
		return err
	}

	pipe := idx.redis.Pipeline()
	for _, b := range blocks {
		bj, _ := json.Marshal(b)
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: "blocks:stream",
			ID:     "*",
			Values: map[string]any{"blockData": string(bj)},
		})
	}
	for _, t := range transactions {
		tj, _ := json.Marshal(t)
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: "transactions:stream",
			ID:     "*",
			Values: map[string]any{"txData": string(tj)},
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("redis xadd", "err", err)
	}

	idx.incBlocks(int64(len(blocks)))
	idx.incTxs(int64(len(transactions)))
	idx.incLogs(int64(len(logs)))

	slog.Info("indexed", "blocks", len(blocks), "txs", len(transactions), "logs", len(logs), "up_to", to)
	return nil
}

type Block struct {
	Hash              string   `json:"hash"`
	Number            int64    `json:"number"`
	Timestamp         int64    `json:"timestamp"`
	Miner             string   `json:"miner"`
	GasUsed           string   `json:"gasUsed"`
	GasLimit          string   `json:"gasLimit"`
	Transactions      []string `json:"transactions"`
	TransactionsCount int      `json:"-"`
}

type Transaction struct {
	Hash            string  `json:"hash"`
	BlockNumber     int64   `json:"blockNumber"`
	BlockHash       string  `json:"blockHash"`
	BlockTimestamp  int64   `json:"blockTimestamp"`
	From            string  `json:"from"`
	To              *string `json:"to"`
	Value           string  `json:"value"`
	Gas             string  `json:"gas"`
	GasPrice        string  `json:"gasPrice"`
	GasUsed         *string `json:"gasUsed"`
	Status          *int    `json:"status"`
	Input           string  `json:"input"`
	Nonce           int64   `json:"nonce"`
	ContractAddress *string `json:"contractAddress"`
}

type Log struct {
	TxHash      string
	BlockNumber int64
	BlockHash   string
	LogIndex    int64
	Address     string
	Topics      []string
	Data        string
	Removed     bool
}

type Address struct {
	Address string
	Count   int
}

func (idx *Indexer) normalizeBlock(b gjson.Result) Block {
	var txs []string
	b.Get("transactions").ForEach(func(_, tx gjson.Result) bool {
		txs = append(txs, strings.ToLower(tx.Get("hash").String()))
		return true
	})
	return Block{
		Hash:              strings.ToLower(b.Get("hash").String()),
		Number:            parseHex(b.Get("number").String()),
		Timestamp:         parseHex(b.Get("timestamp").String()),
		Miner:             strings.ToLower(b.Get("miner").String()),
		GasUsed:           hexToDec(b.Get("gasUsed").String()),
		GasLimit:          hexToDec(b.Get("gasLimit").String()),
		Transactions:      txs,
		TransactionsCount: len(txs),
	}
}

func (idx *Indexer) normalizeTransaction(tx gjson.Result, blockHash string, blockNumber, blockTimestamp int64, receipts map[string]gjson.Result) Transaction {
	hash := strings.ToLower(tx.Get("hash").String())
	r := receipts[hash]
	to := tx.Get("to").String()
	var toPtr *string
	if to != "" {
		to = strings.ToLower(to)
		toPtr = &to
	}
	value := hexToDec(tx.Get("value").String())
	gas := hexToDec(tx.Get("gas").String())
	gasPrice := hexToDec(tx.Get("gasPrice").String())
	if gasPrice == "0" {
		gasPrice = hexToDec(tx.Get("maxFeePerGas").String())
	}
	var gasUsed *string
	if gu := r.Get("gasUsed").String(); gu != "" {
		v := hexToDec(gu)
		gasUsed = &v
	}
	var status *int
	if s := r.Get("status").String(); s != "" {
		si := int(parseHex(s))
		status = &si
	}
	contract := r.Get("contractAddress").String()
	var contractPtr *string
	if contract != "" {
		contract = strings.ToLower(contract)
		contractPtr = &contract
	}
	input := tx.Get("input").String()
	if input == "" {
		input = "0x"
	}
	return Transaction{
		Hash:            hash,
		BlockNumber:     blockNumber,
		BlockHash:       strings.ToLower(blockHash),
		BlockTimestamp:  blockTimestamp,
		From:            strings.ToLower(tx.Get("from").String()),
		To:              toPtr,
		Value:           value,
		Gas:             gas,
		GasPrice:        gasPrice,
		GasUsed:         gasUsed,
		Status:          status,
		Input:           input,
		Nonce:           parseHex(tx.Get("nonce").String()),
		ContractAddress: contractPtr,
	}
}

func (idx *Indexer) normalizeLog(l gjson.Result, txHash string, blockNumber int64, blockHash string) Log {
	var topics []string
	l.Get("topics").ForEach(func(_, t gjson.Result) bool {
		topics = append(topics, strings.ToLower(t.String()))
		return true
	})
	return Log{
		TxHash:      strings.ToLower(txHash),
		BlockNumber: blockNumber,
		BlockHash:   strings.ToLower(blockHash),
		LogIndex:    parseHex(l.Get("logIndex").String()),
		Address:     strings.ToLower(l.Get("address").String()),
		Topics:      topics,
		Data:        l.Get("data").String(),
		Removed:     l.Get("removed").Bool(),
	}
}

func (idx *Indexer) writeBatch(ctx context.Context, blocks []Block, txs []Transaction, logs []Log, addrs []Address) error {
	tx, err := idx.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if len(blocks) > 0 {
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"blocks_staging"}, []string{"hash", "number", "timestamp", "miner", "gas_used", "gas_limit", "transactions_count"}, pgx.CopyFromSlice(len(blocks), func(i int) ([]any, error) {
			b := blocks[i]
			return []any{b.Hash, b.Number, b.Timestamp, b.Miner, b.GasUsed, b.GasLimit, b.TransactionsCount}, nil
		}))
		if err != nil {
			return fmt.Errorf("copy blocks_staging: %w", err)
		}
	}

	if len(txs) > 0 {
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"transactions_staging"}, []string{"hash", "block_number", "block_hash", "block_timestamp", "from_address", "to_address", "value", "gas", "gas_price", "gas_used", "status", "input_data", "nonce", "contract_address"}, pgx.CopyFromSlice(len(txs), func(i int) ([]any, error) {
			t := txs[i]
			return []any{t.Hash, t.BlockNumber, t.BlockHash, t.BlockTimestamp, t.From, t.To, t.Value, t.Gas, t.GasPrice, t.GasUsed, t.Status, t.Input, t.Nonce, t.ContractAddress}, nil
		}))
		if err != nil {
			return fmt.Errorf("copy transactions_staging: %w", err)
		}
	}

	if len(logs) > 0 {
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"logs_staging"}, []string{"transaction_hash", "block_number", "block_hash", "log_index", "address", "topics", "data", "removed"}, pgx.CopyFromSlice(len(logs), func(i int) ([]any, error) {
			l := logs[i]
			topics, err := json.Marshal(l.Topics)
			if err != nil {
				return nil, err
			}
			return []any{l.TxHash, l.BlockNumber, l.BlockHash, l.LogIndex, l.Address, topics, l.Data, l.Removed}, nil
		}))
		if err != nil {
			return fmt.Errorf("copy logs_staging: %w", err)
		}
	}

	if len(addrs) > 0 {
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"addresses_staging"}, []string{"address", "transaction_count"}, pgx.CopyFromSlice(len(addrs), func(i int) ([]any, error) {
			a := addrs[i]
			return []any{a.Address, a.Count}, nil
		}))
		if err != nil {
			return fmt.Errorf("copy addresses_staging: %w", err)
		}
	}

	mergeSQL := `
		INSERT INTO blocks (hash, number, timestamp, miner, gas_used, gas_limit, transactions_count)
		SELECT hash, number, timestamp, miner, gas_used::bigint, gas_limit::bigint, transactions_count FROM blocks_staging
		ON CONFLICT (hash) DO UPDATE SET transactions_count = EXCLUDED.transactions_count
			WHERE blocks.transactions_count = 0 AND EXCLUDED.transactions_count > 0;

		INSERT INTO transactions (hash, block_number, block_hash, block_timestamp, from_address, to_address, value, gas, gas_price, gas_used, status, input_data, nonce, contract_address)
		SELECT hash, block_number, block_hash, block_timestamp, from_address, to_address, value::decimal(38,0), gas::bigint, gas_price::decimal(38,0), gas_used::bigint, status, input_data, nonce, contract_address FROM transactions_staging
		ON CONFLICT (hash) DO UPDATE SET
			contract_address = EXCLUDED.contract_address
			WHERE transactions.contract_address IS NULL;

		INSERT INTO logs (transaction_hash, block_number, block_hash, log_index, address, topics, data, removed)
		SELECT transaction_hash, block_number, block_hash, log_index::int, address, topics, data, removed FROM logs_staging
		ON CONFLICT (transaction_hash, log_index) DO NOTHING;

		INSERT INTO addresses (address, transaction_count, first_seen, last_seen)
		SELECT address, transaction_count, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP FROM addresses_staging
		ON CONFLICT (address) DO UPDATE SET
			transaction_count = addresses.transaction_count + EXCLUDED.transaction_count,
			last_seen = CURRENT_TIMESTAMP;

		TRUNCATE blocks_staging, transactions_staging, logs_staging, addresses_staging;
	`
	_, err = tx.Exec(ctx, mergeSQL)
	if err != nil {
		return fmt.Errorf("merge staging: %w", err)
	}

	return tx.Commit(ctx)
}

func parseHex(s string) int64 {
	if s == "" {
		return 0
	}
	s = strings.TrimPrefix(s, "0x")
	n, _ := new(big.Int).SetString(s, 16)
	if n == nil {
		return 0
	}
	return n.Int64()
}

func hexToDec(s string) string {
	if s == "" {
		return "0"
	}
	s = strings.TrimPrefix(s, "0x")
	n, _ := new(big.Int).SetString(s, 16)
	if n == nil {
		return "0"
	}
	return n.String()
}

func (idx *Indexer) incErrors() {
	idx.mu.Lock()
	idx.stats.Errors++
	idx.mu.Unlock()
}

func (idx *Indexer) incBlocks(n int64) {
	idx.mu.Lock()
	idx.stats.BlocksProcessed += n
	idx.mu.Unlock()
}

func (idx *Indexer) incTxs(n int64) {
	idx.mu.Lock()
	idx.stats.TransactionsProcessed += n
	idx.mu.Unlock()
}

func (idx *Indexer) incLogs(n int64) {
	idx.mu.Lock()
	idx.stats.LogsProcessed += n
	idx.mu.Unlock()
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS blocks_staging (
    hash VARCHAR(66) NOT NULL,
    number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    miner VARCHAR(42) NOT NULL,
    gas_used TEXT,
    gas_limit TEXT,
    transactions_count INT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions_staging (
    hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(66),
    block_timestamp BIGINT,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42),
    value TEXT,
    gas TEXT,
    gas_price TEXT,
    gas_used TEXT,
    status SMALLINT,
    input_data TEXT,
    nonce BIGINT,
    contract_address VARCHAR(42)
);

CREATE TABLE IF NOT EXISTS logs_staging (
    transaction_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(66),
    log_index BIGINT NOT NULL,
    address VARCHAR(42) NOT NULL,
    topics JSONB,
    data TEXT,
    removed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS addresses_staging (
    address VARCHAR(42) NOT NULL,
    transaction_count INT NOT NULL
);

TRUNCATE blocks_staging, transactions_staging, logs_staging, addresses_staging;
`
