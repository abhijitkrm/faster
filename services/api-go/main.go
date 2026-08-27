package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/gjson"
)

type Config struct {
	Port        string
	RPCURL      string
	RedisURL    string
	DatabaseURL string
	LogLevel    string
}

func loadConfig() Config {
	return Config{
		Port:        getEnv("PORT", "3000"),
		RPCURL:      getEnv("RPC_URL", "http://localhost:8545"),
		RedisURL:    getEnv("REDIS_URL", "redis://localhost:6379"),
		DatabaseURL: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/explorer"),
		LogLevel:    getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type Server struct {
	cfg       Config
	db        *pgxpool.Pool
	rdb       *redis.Client
	rpc       *http.Client
	upgrader  websocket.Upgrader
	clients   map[*websocket.Conn]bool
	clientsMu sync.Mutex
	lastBlock string
	lastTx    string
}

func (s *Server) queryRowMap(ctx context.Context, sql string, args ...any) (map[string]any, error) {
	rows, err := s.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	res, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, pgx.ErrNoRows
	}
	return res[0], nil
}

func main() {
	cfg := loadConfig()
	level := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	ctx := context.Background()

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("postgres connect", "err", err)
		os.Exit(1)
	}
	defer db.Close()

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

	s := &Server{
		cfg:     cfg,
		db:      db,
		rdb:     rdb,
		rpc:     &http.Client{Timeout: 10 * time.Second},
		clients: make(map[*websocket.Conn]bool),
		lastBlock: "$",
		lastTx:    "$",
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}

	go s.streamLoop(ctx)
	go s.statsHeartbeat(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/api/blocks", s.handleBlocks)
	mux.HandleFunc("/api/blocks/", s.handleBlocks)
	mux.HandleFunc("/api/transactions", s.handleTransactions)
	mux.HandleFunc("/api/transactions/", s.handleTransactions)
	mux.HandleFunc("/api/addresses", s.handleAddresses)
	mux.HandleFunc("/api/addresses/", s.handleAddresses)
	mux.HandleFunc("/", s.handleRoot) // WebSocket on /, 404 otherwise

	listener, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	slog.Info("api-go listening", "port", cfg.Port)
	if err := http.Serve(listener, withCORS(mux)); err != nil {
		slog.Error("serve", "err", err)
		os.Exit(1)
	}
}

func withCORS(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Header.Get("Upgrade") == "websocket" {
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			slog.Warn("ws upgrade", "err", err)
			return
		}
		s.handleConnection(conn)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleConnection(conn *websocket.Conn) {
	s.clientsMu.Lock()
	s.clients[conn] = true
	s.clientsMu.Unlock()

	ctx := context.Background()
	if err := s.sendInit(conn, ctx); err != nil {
		slog.Warn("ws init", "err", err)
	}

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var m struct{ Type string `json:"type"` }
		if json.Unmarshal(msg, &m) == nil && m.Type == "ping" {
			_ = conn.WriteJSON(map[string]string{"type": "pong"})
		}
	}

	s.clientsMu.Lock()
	delete(s.clients, conn)
	s.clientsMu.Unlock()
	_ = conn.Close()
}

func (s *Server) sendInit(conn *websocket.Conn, ctx context.Context) error {
	stats, err := s.getStats(ctx)
	if err != nil {
		return err
	}
	blocks, err := s.getLatestBlocks(ctx, 20)
	if err != nil {
		return err
	}
	txs, err := s.getLatestTxs(ctx, 20)
	if err != nil {
		return err
	}
	return conn.WriteJSON(map[string]any{
		"type": "init",
		"data": map[string]any{
			"stats":  stats,
			"blocks": blocks,
			"txs":    txs,
		},
	})
}

func (s *Server) broadcast(msg any) {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()
	if len(s.clients) == 0 {
		return
	}
	b, _ := json.Marshal(msg)
	for c := range s.clients {
		_ = c.WriteMessage(websocket.TextMessage, b)
	}
}

func (s *Server) processBlock(msg redis.XMessage) {
	s.lastBlock = msg.ID
	b, _ := msg.Values["blockData"].(string)
	if b == "" {
		return
	}
	var block struct {
		Hash         string   `json:"hash"`
		Number       int64    `json:"number"`
		Timestamp    int64    `json:"timestamp"`
		Miner        string   `json:"miner"`
		GasUsed      string   `json:"gasUsed"`
		Transactions []string `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(b), &block); err != nil {
		return
	}
	s.broadcast(map[string]any{
		"type": "block",
		"data": map[string]any{
			"hash":               block.Hash,
			"number":             block.Number,
			"timestamp":          block.Timestamp,
			"miner":              block.Miner,
			"gas_used":           block.GasUsed,
			"transactions_count": len(block.Transactions),
		},
	})
}

func (s *Server) processTx(msg redis.XMessage) {
	s.lastTx = msg.ID
	t, _ := msg.Values["txData"].(string)
	if t == "" {
		return
	}
	var tx struct {
		Hash        string  `json:"hash"`
		BlockNumber int64   `json:"blockNumber"`
		From        string  `json:"from"`
		To          *string `json:"to"`
		Value       string  `json:"value"`
		Status      *int    `json:"status"`
	}
	if err := json.Unmarshal([]byte(t), &tx); err != nil {
		return
	}
	s.broadcast(map[string]any{
		"type": "tx",
		"data": map[string]any{
			"hash":         tx.Hash,
			"block_number": tx.BlockNumber,
			"from_address": tx.From,
			"to_address":   tx.To,
			"value":        tx.Value,
			"status":       tx.Status,
		},
	})
}

func (s *Server) streamLoop(ctx context.Context) {
	for {
		hasData := false
		hasNewBlocks := false

		blockResult, err := s.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"blocks:stream", s.lastBlock},
			Count:   1000,
		}).Result()
		if err != nil && err != redis.Nil {
			slog.Error("xread blocks", "err", err)
		} else if len(blockResult) > 0 {
			for _, msg := range blockResult[0].Messages {
				s.processBlock(msg)
				hasNewBlocks = true
				hasData = true
			}
		}

		txResult, err := s.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"transactions:stream", s.lastTx},
			Count:   1000,
		}).Result()
		if err != nil && err != redis.Nil {
			slog.Error("xread txs", "err", err)
		} else if len(txResult) > 0 {
			for _, msg := range txResult[0].Messages {
				s.processTx(msg)
				hasData = true
			}
		}

		if hasNewBlocks {
			if stats, err := s.getStats(ctx); err == nil {
				s.broadcast(map[string]any{"type": "stats", "data": stats})
			}
		}

		if !hasData {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (s *Server) statsHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		stats, err := s.getStats(ctx)
		if err == nil {
			s.broadcast(map[string]any{"type": "stats", "data": stats})
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.clientsMu.Lock()
	n := len(s.clients)
	s.clientsMu.Unlock()
	json.NewEncoder(w).Encode(map[string]any{
		"status":           "ok",
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
		"connectedClients": n,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.getStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) getStats(ctx context.Context) (map[string]any, error) {
	statsRow := s.db.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*)          FROM blocks)::text        AS total_blocks,
		  (SELECT MAX(number)::text  FROM blocks)             AS block_height,
		  (SELECT MAX(timestamp)::text FROM blocks)           AS latest_block_timestamp,
		  (SELECT COUNT(*)          FROM transactions)::text  AS total_transactions,
		  (SELECT COUNT(*)          FROM addresses)::text     AS total_addresses,
		  (SELECT COUNT(*)          FROM logs)::text          AS total_logs
	`)
	var stats struct {
		TotalBlocks          string
		BlockHeight          *string
		LatestBlockTimestamp *string
		TotalTransactions    string
		TotalAddresses       string
		TotalLogs            string
	}
	if err := statsRow.Scan(&stats.TotalBlocks, &stats.BlockHeight, &stats.LatestBlockTimestamp, &stats.TotalTransactions, &stats.TotalAddresses, &stats.TotalLogs); err != nil {
		return nil, err
	}

	var blockCount, timeSpan *int64
	err := s.db.QueryRow(ctx, `
		SELECT
		  (MAX(number)::bigint    - MIN(number)::bigint)    AS block_count,
		  (MAX(timestamp)::bigint - MIN(timestamp)::bigint) AS time_span
		FROM (
		  SELECT number, timestamp::bigint
		  FROM blocks ORDER BY number DESC LIMIT 101
		) t
	`).Scan(&blockCount, &timeSpan)
	if err != nil {
		return nil, err
	}

	var tpsBlockCount, tpsTimeSpan, latestTxns *int64
	err = s.db.QueryRow(ctx, `
		SELECT
		  (MAX(number)::bigint    - MIN(number)::bigint)    AS block_count,
		  (MAX(timestamp)::bigint - MIN(timestamp)::bigint) AS time_span,
		  (SELECT transactions_count FROM blocks ORDER BY number DESC LIMIT 1) AS latest_txns
		FROM (
		  SELECT number, timestamp::bigint
		  FROM blocks ORDER BY number DESC LIMIT 20
		) t
	`).Scan(&tpsBlockCount, &tpsTimeSpan, &latestTxns)
	if err != nil {
		return nil, err
	}

	avg := "0.000"
	if blockCount != nil && timeSpan != nil && *blockCount > 0 && *timeSpan > 0 {
		avg = fmt.Sprintf("%.3f", float64(*timeSpan)/float64(*blockCount))
	}

	tps := "0.00"
	if tpsBlockCount != nil && tpsTimeSpan != nil && *tpsBlockCount > 0 && *tpsTimeSpan > 0 && latestTxns != nil {
		blockTime := float64(*tpsTimeSpan) / float64(*tpsBlockCount)
		tps = fmt.Sprintf("%.2f", float64(*latestTxns)/blockTime)
	}

	return map[string]any{
		"total_blocks":           stats.TotalBlocks,
		"block_height":           nilOrString(stats.BlockHeight),
		"latest_block_timestamp": nilOrString(stats.LatestBlockTimestamp),
		"total_transactions":     stats.TotalTransactions,
		"total_addresses":        stats.TotalAddresses,
		"total_logs":             stats.TotalLogs,
		"avg_block_time":         avg,
		"tps":                    tps,
	}, nil
}

func nilOrString(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

func (s *Server) getLatestBlocks(ctx context.Context, limit int) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT hash, number, timestamp, miner, transactions_count, gas_used
		FROM blocks ORDER BY number DESC LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToMap)
}

func (s *Server) getLatestTxs(ctx context.Context, limit int) ([]map[string]any, error) {
	rows, err := s.db.Query(ctx, `
		SELECT hash, block_number, from_address, to_address, value, status
		FROM transactions ORDER BY id DESC LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowToMap)
}

func (s *Server) handleBlocks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	path := strings.TrimPrefix(r.URL.Path, "/api/blocks")
	path = strings.TrimPrefix(path, "/")

	if path == "" {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 100 {
			limit = 20
		}
		rows, err := s.db.Query(ctx, `SELECT * FROM blocks ORDER BY number DESC LIMIT $1`, limit)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer rows.Close()
		res, err := pgx.CollectRows(rows, pgx.RowToMap)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(res)
		return
	}

	parts := strings.Split(path, "/")
	if len(parts) >= 3 && parts[0] == "range" {
		// /api/blocks/range/:start/:end
		start, _ := strconv.ParseInt(parts[1], 10, 64)
		end, _ := strconv.ParseInt(parts[2], 10, 64)
		if start > end || end-start > 1000 {
			http.Error(w, `{"error":"Invalid range"}`, 400)
			return
		}
		rows, err := s.db.Query(ctx, `
			SELECT number, hash, timestamp, miner, transactions_count, gas_used, gas_limit
			FROM blocks WHERE number BETWEEN $1 AND $2 ORDER BY number ASC
		`, start, end)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer rows.Close()
		res, err := pgx.CollectRows(rows, pgx.RowToMap)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(res)
		return
	}

	// /api/blocks/:number or :hash
	param := parts[0]
	var block map[string]any
	var err error
	if strings.HasPrefix(param, "0x") {
		block, err = s.queryRowMap(ctx, `SELECT * FROM blocks WHERE hash = $1`, param)
	} else {
		n, _ := strconv.ParseInt(param, 10, 64)
		block, err = s.queryRowMap(ctx, `SELECT * FROM blocks WHERE number = $1`, n)
	}
	if err == pgx.ErrNoRows {
		http.Error(w, `{"error":"Block not found"}`, 404)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	bnum, _ := block["number"].(int64)
	rows, err := s.db.Query(ctx, `
		SELECT hash, from_address, to_address, value, gas, gas_price, gas_used, status, nonce, input_data, block_timestamp
		FROM transactions WHERE block_number = $1 ORDER BY nonce ASC
	`, bnum)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()
	txs, _ := pgx.CollectRows(rows, pgx.RowToMap)
	block["transactions"] = txs
	json.NewEncoder(w).Encode(block)
}

func (s *Server) handleTransactions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	path := strings.TrimPrefix(r.URL.Path, "/api/transactions")
	path = strings.TrimPrefix(path, "/")

	if path == "" {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 100 {
			limit = 20
		}
		rows, err := s.db.Query(ctx, `
			SELECT hash, block_number, from_address, to_address, value, gas_price, status, created_at
			FROM transactions ORDER BY created_at DESC LIMIT $1
		`, limit)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer rows.Close()
		res, _ := pgx.CollectRows(rows, pgx.RowToMap)
		json.NewEncoder(w).Encode(res)
		return
	}

	parts := strings.Split(path, "/")
	if len(parts) == 2 && parts[0] == "address" {
		addr := strings.ToLower(parts[1])
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 100 {
			limit = 20
		}
		rows, err := s.db.Query(ctx, `
			SELECT hash, block_number, from_address, to_address, value, gas_price, status, created_at
			FROM transactions
			WHERE from_address = $1 OR to_address = $1
			ORDER BY created_at DESC LIMIT $2
		`, addr, limit)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer rows.Close()
		res, _ := pgx.CollectRows(rows, pgx.RowToMap)
		json.NewEncoder(w).Encode(res)
		return
	}

	if path == "pending/list" {
		rows, err := s.db.Query(ctx, `
			SELECT hash, from_address, to_address, value, gas_price
			FROM transactions WHERE status IS NULL ORDER BY gas_price DESC LIMIT 20
		`)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer rows.Close()
		res, _ := pgx.CollectRows(rows, pgx.RowToMap)
		json.NewEncoder(w).Encode(res)
		return
	}

	// /api/transactions/:hash
	hash := strings.ToLower(parts[0])
	tx, err := s.queryRowMap(ctx, `SELECT * FROM transactions WHERE hash = $1`, hash)
	if err == pgx.ErrNoRows {
		http.Error(w, `{"error":"Transaction not found"}`, 404)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	logs, err := s.db.Query(ctx, `
		SELECT log_index, address, topics, data
		FROM logs WHERE transaction_hash = $1 ORDER BY log_index ASC
	`, hash)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer logs.Close()
	logRows, _ := pgx.CollectRows(logs, pgx.RowToMap)
	tx["logs"] = logRows
	json.NewEncoder(w).Encode(tx)
}

func (s *Server) handleAddresses(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	path := strings.TrimPrefix(r.URL.Path, "/api/addresses")
	path = strings.TrimPrefix(path, "/")

	if path == "" {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 100 {
			limit = 20
		}
		rows, err := s.db.Query(ctx, `
			SELECT address, transaction_count, first_seen, last_seen, is_contract
			FROM addresses ORDER BY transaction_count DESC LIMIT $1
		`, limit)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer rows.Close()
		res, _ := pgx.CollectRows(rows, pgx.RowToMap)
		json.NewEncoder(w).Encode(res)
		return
	}

	parts := strings.Split(path, "/")
	if len(parts) == 2 && parts[0] == "search" && parts[1] == "by-pattern" {
		pattern := r.URL.Query().Get("pattern")
		if len(pattern) < 3 {
			http.Error(w, `{"error":"Pattern must be at least 3 characters"}`, 400)
			return
		}
		rows, err := s.db.Query(ctx, `
			SELECT address, transaction_count, last_seen
			FROM addresses
			WHERE address ILIKE $1
			LIMIT 20
		`, pattern+"%")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer rows.Close()
		res, _ := pgx.CollectRows(rows, pgx.RowToMap)
		json.NewEncoder(w).Encode(res)
		return
	}

	// /api/addresses/:address
	addr := strings.ToLower(parts[0])
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 50 {
		limit = 25
	}
	offset := (page - 1) * limit

	addrRow, _ := s.queryRowMap(ctx, `
		SELECT address, transaction_count, first_seen, last_seen, is_contract
		FROM addresses WHERE address = $1
	`, addr)

	rows, err := s.db.Query(ctx, `
		SELECT t.hash, t.block_number, t.block_timestamp,
		       t.from_address, t.to_address, t.value, t.gas, t.gas_price, t.gas_used,
		       t.status, t.nonce, t.input_data
		FROM transactions t
		WHERE t.from_address = $1 OR t.to_address = $1
		ORDER BY t.block_number DESC, t.nonce DESC
		LIMIT $2 OFFSET $3
	`, addr, limit, offset)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()
	txs, _ := pgx.CollectRows(rows, pgx.RowToMap)

	var total int
	err = s.db.QueryRow(ctx, `SELECT COUNT(*)::int FROM transactions WHERE from_address = $1 OR to_address = $1`, addr).Scan(&total)
	if err != nil {
		total = 0
	}

	balance := s.rpcBalance(addr)
	code := s.rpcCode(addr)

	deployInfo := s.getDeployInfo(ctx, addr)

	if addrRow == nil {
		json.NewEncoder(w).Encode(map[string]any{
			"address":          addr,
			"transaction_count": 0,
			"first_seen":       nil,
			"last_seen":        nil,
			"is_contract":      code.isContract,
			"bytecode_size":    code.bytecodeSize,
			"bytecode":         code.bytecode,
			"deploy_info":      deployInfo,
			"balance":          balance,
			"transactions":     txs,
			"total_transactions": total,
			"page":             page,
			"limit":            limit,
		})
		return
	}

	isContract := false
	if v, ok := addrRow["is_contract"].(bool); ok {
		isContract = v
	}
	if code.isContract {
		isContract = true
	}
	addrRow["is_contract"] = isContract
	addrRow["bytecode_size"] = code.bytecodeSize
	addrRow["bytecode"] = code.bytecode
	addrRow["deploy_info"] = deployInfo
	addrRow["balance"] = balance
	addrRow["transactions"] = txs
	addrRow["total_transactions"] = total
	addrRow["page"] = page
	addrRow["limit"] = limit
	json.NewEncoder(w).Encode(addrRow)
}

func (s *Server) getDeployInfo(ctx context.Context, addr string) any {
	res, err := s.queryRowMap(ctx, `
		SELECT hash, block_number, block_timestamp, from_address
		FROM transactions WHERE contract_address = $1
		ORDER BY block_number ASC LIMIT 1
	`, addr)
	if err == nil {
		return res
	}
	return nil
}

func (s *Server) rpcBalance(addr string) string {
	res, err := s.rpcCall("eth_getBalance", []any{addr, "latest"})
	if err != nil || res.String() == "" {
		return "0"
	}
	return hexToDec(res.String())
}

type codeInfo struct {
	isContract  bool
	bytecodeSize int
	bytecode    string
}

func (s *Server) rpcCode(addr string) codeInfo {
	res, err := s.rpcCall("eth_getCode", []any{addr, "latest"})
	if err != nil {
		return codeInfo{}
	}
	r := res.String()
	if r == "" || r == "0x" || r == "0x0" {
		return codeInfo{}
	}
	return codeInfo{
		isContract:   true,
		bytecodeSize: (len(r) - 2) / 2,
		bytecode:     r,
	}
}

func (s *Server) rpcCall(method string, params []any) (gjson.Result, error) {
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
	req, err := http.NewRequest("POST", s.cfg.RPCURL, strings.NewReader(string(b)))
	if err != nil {
		return gjson.Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.rpc.Do(req)
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

func hexToDec(s string) string {
	if s == "" {
		return "0"
	}
	s = strings.TrimPrefix(s, "0x")
	n, ok := new(big.Int).SetString(s, 16)
	if !ok {
		return "0"
	}
	return n.String()
}
