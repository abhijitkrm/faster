package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// nodeClient talks to the node's clob JSON-RPC namespace.
type nodeClient struct {
	url string
	hc  *http.Client
	id  atomic.Uint64
}

func newNodeClient(url string, timeout time.Duration) *nodeClient {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = 64
	return &nodeClient{url: url, hc: &http.Client{Timeout: timeout, Transport: tr}}
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message) }

func (c *nodeClient) call(ctx context.Context, method string, params []any) (json.RawMessage, error) {
	if params == nil {
		params = []any{}
	}
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": c.id.Add(1), "method": method, "params": params,
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: http %d: %.200s", method, resp.StatusCode, data)
	}
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcError       `json:"error"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("%s: decode response: %w", method, err)
	}
	if out.Error != nil {
		return nil, fmt.Errorf("%s: %w", method, out.Error)
	}
	return out.Result, nil
}

func (c *nodeClient) archiveLatest(ctx context.Context) (ArchiveInfo, error) {
	var info ArchiveInfo
	raw, err := c.call(ctx, "clob_archiveLatest", nil)
	if err != nil {
		return info, err
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return info, fmt.Errorf("clob_archiveLatest: %w", err)
	}
	return info, nil
}

// blockArchive returns the archive for height h, or nil when the node does
// not hold it.
func (c *nodeClient) blockArchive(ctx context.Context, h uint64) (*Block, error) {
	raw, err := c.call(ctx, "clob_getBlockArchive", []any{h})
	if err != nil {
		return nil, err
	}
	return decodeBlock(raw, h)
}
