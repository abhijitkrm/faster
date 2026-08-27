package main

import (
	"encoding/json"
	"testing"

	"github.com/tidwall/gjson"
)

const sampleBlock = `{
  "hash": "0xfe00f7d49cfbbf3faf72eb52c3db70e21ce148d4f09409841476ce6e419538ab",
  "number": "0x1208348f",
  "timestamp": "0x6a901415",
  "miner": "0x9085a398d1d43391bf6ab59254702beed39a5b32",
  "gasUsed": "0x5208",
  "gasLimit": "0x1c9c380",
  "transactions": [
    {
      "hash": "0x2dc05b21a014a93b3b2e66ae9cb0ffb4171c21ed4d2d73737a30a524c72ec8c3",
      "from": "0x5fcab653b4537576c12de0adef467c0a2a42025c",
      "to": "0xa1467ffdc95cd2821736ee5b044e14e72cb80fd4",
      "value": "0x0",
      "gas": "0x9eb10",
      "gasPrice": "0x14159a0",
      "nonce": "0x7da36",
      "input": "0x2518d538"
    }
  ]
}`

const sampleReceipt = `{
  "transactionHash": "0x2dc05b21a014a93b3b2e66ae9cb0ffb4171c21ed4d2d73737a30a524c72ec8c3",
  "gasUsed": "0x9eb10",
  "status": "0x1",
  "logs": [
    {
      "address": "0xa12648f038bea7c9fa4517240e4c89bf89511e4b",
      "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x000000000000000000000000c31a1241bb37993a125f417c8ee6e3563042d16c",
        "0x0000000000000000000000002b96f276140ea7e993a6720bfda919565a93e1a6"
      ],
      "data": "0x0000000000000000000000000000000000000000000000000000000005f5e100",
      "logIndex": "0x25",
      "removed": false
    }
  ]
}`

func BenchmarkHexToDec(b *testing.B) {
	samples := []string{
		"0x5208",
		"0x1c9c380",
		"0x14159a0",
		"0x7da36",
		"0x1208348f",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, s := range samples {
			hexToDec(s)
		}
	}
}

func BenchmarkNormalizeBlock(b *testing.B) {
	block := gjson.Parse(sampleBlock)
	idx := &Indexer{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.normalizeBlock(block)
	}
}

func BenchmarkNormalizeTransaction(b *testing.B) {
	var block struct {
		Hash      string `json:"hash"`
		Number    string `json:"number"`
		Timestamp string `json:"timestamp"`
	}
	_ = json.Unmarshal([]byte(sampleBlock), &block)

	blockHash := block.Hash
	blockNumber := parseHex(block.Number)
	blockTimestamp := parseHex(block.Timestamp)

	tx := gjson.Get(sampleBlock, "transactions.0")
	receipt := gjson.Parse(sampleReceipt)
	receiptMap := map[string]gjson.Result{
		receipt.Get("transactionHash").String(): receipt,
	}

	idx := &Indexer{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.normalizeTransaction(tx, blockHash, blockNumber, blockTimestamp, receiptMap)
	}
}

func BenchmarkNormalizeLog(b *testing.B) {
	log := gjson.Get(sampleReceipt, "logs.0")
	idx := &Indexer{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.normalizeLog(log, "0x2dc05b21a014a93b3b2e66ae9cb0ffb4171c21ed4d2d73737a30a524c72ec8c3", 302527631, "0xfe00f7d49cfbbf3faf72eb52c3db70e21ce148d4f09409841476ce6e419538ab")
	}
}
