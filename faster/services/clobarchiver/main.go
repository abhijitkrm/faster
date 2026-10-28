// Command clobarchiver copies the node's per-block CLOB history (JSON-RPC
// clob_getBlockArchive, node started with CLOB_ARCHIVE=1) into ClickHouse
// (database/clickhouse/01-clob.sql), strictly in height order.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type Config struct {
	NodeRPC         string
	RPCTimeoutMs    int
	CHAddr          string // host:port of the native protocol
	CHDSN           string // clickhouse://user:pass@host:9000/db?...; overrides the fields below
	CHDatabase      string
	CHUser          string
	CHPassword      string
	FetchWorkers    int
	Window          int // max heights fetched ahead of the inserter
	BatchMaxRows    int
	BatchMaxHeights int
	BatchMaxWaitMs  int
	MaxRange        int // heights per archiveLatest refresh
	PollMs          int
	StartHeight     uint64
	MetricsAddr     string
	LogLevel        string
}

func loadConfig() Config {
	return Config{
		NodeRPC:         getEnv("NODE_RPC", "http://127.0.0.1:8545"),
		RPCTimeoutMs:    getEnvInt("RPC_TIMEOUT_MS", 30000),
		CHAddr:          getEnv("CLICKHOUSE_ADDR", "127.0.0.1:9000"),
		CHDSN:           getEnv("CLICKHOUSE_DSN", ""),
		CHDatabase:      getEnv("CLICKHOUSE_DB", "clob"),
		CHUser:          getEnv("CLICKHOUSE_USER", "default"),
		CHPassword:      getEnv("CLICKHOUSE_PASSWORD", ""),
		FetchWorkers:    getEnvInt("FETCH_WORKERS", 8),
		Window:          getEnvInt("FETCH_WINDOW", 0),
		BatchMaxRows:    getEnvInt("BATCH_MAX_ROWS", 100_000),
		BatchMaxHeights: getEnvInt("BATCH_MAX_HEIGHTS", 1000),
		BatchMaxWaitMs:  getEnvInt("BATCH_MAX_WAIT_MS", 2000),
		MaxRange:        getEnvInt("MAX_RANGE", 100_000),
		PollMs:          getEnvInt("POLL_MS", 1000),
		StartHeight:     uint64(getEnvInt("START_HEIGHT", 0)),
		MetricsAddr:     getEnv("METRICS_ADDR", ":9102"),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("bad integer env; using default", "key", key, "value", v, "default", def)
		return def
	}
	return n
}

func main() {
	cfg := loadConfig()
	level := slog.LevelInfo
	if cfg.LogLevel == "debug" {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := OpenClickHouse(ctx, cfg)
	if err != nil {
		slog.Error("clickhouse", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	m := NewMetrics()
	mux := http.NewServeMux()
	mux.Handle("/metrics", m)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","checkpoint":` + strconv.FormatUint(m.checkpoint.Load(), 10) +
			`,"node_latest":` + strconv.FormatUint(m.nodeLatest.Load(), 10) + `}`))
	})
	srv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server", "err", err)
		}
	}()
	go m.logLoop(ctx, 10*time.Second)

	slog.Info("clobarchiver starting", "node", cfg.NodeRPC, "clickhouse", store.Describe(),
		"workers", cfg.FetchWorkers, "batch_max_rows", cfg.BatchMaxRows, "batch_max_heights", cfg.BatchMaxHeights)
	a := NewArchiver(cfg, newNodeClient(cfg.NodeRPC, time.Duration(cfg.RPCTimeoutMs)*time.Millisecond), store, m)
	err = a.Run(ctx)
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	if err != nil {
		slog.Error("archiver stopped", "err", err)
		os.Exit(1)
	}
	slog.Info("clobarchiver stopped", "checkpoint", m.checkpoint.Load())
}
