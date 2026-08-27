import { createClient } from 'redis';
import pg from 'pg';
import pino from 'pino';
import http from 'http';
import dotenv from 'dotenv';

dotenv.config();

const logger = pino({ level: process.env.LOG_LEVEL || 'info' });
const { Pool } = pg;

class Indexer {
  constructor() {
    this.redisUrl    = process.env.REDIS_URL;
    this.databaseUrl = process.env.DATABASE_URL;
    this.batchSize   = parseInt(process.env.BATCH_SIZE  || '100');
    this.poolMax     = parseInt(process.env.DB_POOL_MAX || '20');
    this.redisClient = null;
    this.dbPool = null;
    this.isRunning = false;
    this.stats = {
      blocksProcessed: 0,
      transactionsProcessed: 0,
      logsProcessed: 0,
      errors: 0,
    };
    this.lastBlockId = '0-0';
    this.lastTxId = '0-0';
  }

  async initialize() {
    try {
      // Initialize Redis
      this.redisClient = createClient({ url: this.redisUrl });
      this.redisClient.on('error', (err) => logger.error('Redis error:', err));
      await this.redisClient.connect();
      logger.info('Redis connected');

      // Initialize Database
      this.dbPool = new Pool({
        connectionString: this.databaseUrl,
        max: this.poolMax,
        idleTimeoutMillis: 30000,
      });
      const client = await this.dbPool.connect();
      await client.query('SELECT 1');
      client.release();
      logger.info('Database connected');

      // Restore persisted stream positions (survive restarts/rebuilds)
      const savedBlockId = await this.redisClient.get('indexer:lastBlockId');
      const savedTxId    = await this.redisClient.get('indexer:lastTxId');
      if (savedBlockId) {
        this.lastBlockId = savedBlockId;
        logger.info(`Resuming block stream from ${savedBlockId}`);
      }
      if (savedTxId) {
        this.lastTxId = savedTxId;
        logger.info(`Resuming tx stream from ${savedTxId}`);
      }

      this.isRunning = true;
    } catch (error) {
      logger.error('Initialization error:', error);
      throw error;
    }
  }

  async startIndexing() {
    // Run both stream loops concurrently — each has its own error/backoff handling
    await Promise.all([
      this._blockLoop(),
      this._txLoop(),
    ]);
  }

  async _blockLoop() {
    while (this.isRunning) {
      try {
        await this.processBlockStream();
      } catch (error) {
        logger.error({ err: error }, 'Error in block indexing loop');
        this.stats.errors++;
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  async _txLoop() {
    while (this.isRunning) {
      try {
        await this.processTransactionStream();
      } catch (error) {
        logger.error({ err: error }, 'Error in tx indexing loop');
        this.stats.errors++;
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  async processBlockStream() {
    const result = await this.redisClient.xRead(
      [{ key: 'blocks:stream', id: this.lastBlockId }],
      { COUNT: this.batchSize, BLOCK: 1000 }
    );
    if (!result || result.length === 0) return;

    const messages = result[0].messages;

    // Parse all messages; skip any that fail JSON decode
    const parsed = [];
    for (const { id, message } of messages) {
      try {
        parsed.push({ id, block: JSON.parse(message.blockData) });
      } catch (e) {
        logger.error(`Failed to parse block message ${id}: ${e.message}`);
      }
    }
    if (parsed.length === 0) return;

    // Build a single multi-row INSERT for the whole batch
    const placeholders = [];
    const params = [];
    for (let i = 0; i < parsed.length; i++) {
      const b = parsed[i].block;
      const o = i * 7;
      placeholders.push(
        `($${o+1},$${o+2},$${o+3},$${o+4},$${o+5},$${o+6},$${o+7})`
      );
      params.push(
        b.hash, b.number, b.timestamp, b.miner,
        b.gasUsed, b.gasLimit, b.transactions.length
      );
    }

    const client = await this.dbPool.connect();
    try {
      await client.query(
        `INSERT INTO blocks (hash, number, timestamp, miner, gas_used, gas_limit, transactions_count)
         VALUES ${placeholders.join(',')}
         ON CONFLICT (hash) DO UPDATE
           SET transactions_count = EXCLUDED.transactions_count
           WHERE blocks.transactions_count = 0 AND EXCLUDED.transactions_count > 0`,
        params
      );

      this.lastBlockId = parsed[parsed.length - 1].id;
      this.stats.blocksProcessed += parsed.length;
      await this.redisClient.set('indexer:lastBlockId', this.lastBlockId);
      logger.info(`Indexed ${parsed.length} blocks`);
    } finally {
      client.release();
    }
  }

  async processTransactionStream() {
    const result = await this.redisClient.xRead(
      [{ key: 'transactions:stream', id: this.lastTxId }],
      { COUNT: this.batchSize, BLOCK: 1000 }
    );
    if (!result || result.length === 0) return;

    const messages = result[0].messages;

    // Parse all messages; skip any that fail JSON decode
    const parsed = [];
    for (const { id, message } of messages) {
      try {
        parsed.push({ id, tx: JSON.parse(message.txData) });
      } catch (e) {
        logger.error(`Failed to parse tx message ${id}: ${e.message}`);
      }
    }
    if (parsed.length === 0) return;

    // ── 1. Build batch tx INSERT ──────────────────────────────────────────────
    const txPlaceholders = [];
    const txParams = [];
    for (let i = 0; i < parsed.length; i++) {
      const tx = parsed[i].tx;
      const o  = i * 13;
      txPlaceholders.push(
        `($${o+1},$${o+2},$${o+3},$${o+4},$${o+5},$${o+6},$${o+7},$${o+8},$${o+9},$${o+10},$${o+11},$${o+12},$${o+13})`
      );
      txParams.push(
        tx.hash, tx.blockNumber, tx.blockHash, tx.from, tx.to,
        tx.value, tx.gas, tx.gasPrice, tx.gasUsed, tx.status,
        tx.input, tx.nonce, tx.contractAddress || null
      );
    }

    // ── 2. Collect all logs from the batch ────────────────────────────────────
    const allLogs = [];
    for (const { tx } of parsed) {
      if (tx.logs && tx.logs.length > 0) {
        for (const log of tx.logs) {
          const rawIndex = log.logIndex ?? log.index;
          const logIndex = rawIndex != null ? Number(rawIndex) : null;
          if (logIndex == null) continue;
          allLogs.push({
            txHash: tx.hash, blockNumber: tx.blockNumber,
            blockHash: tx.blockHash, index: logIndex,
            address: log.address, data: JSON.stringify(log),
          });
        }
      }
    }

    // ── 3. Accumulate per-address tx counts across the batch ─────────────────
    const addressCounts = new Map();
    for (const { tx } of parsed) {
      if (tx.from) addressCounts.set(tx.from, (addressCounts.get(tx.from) || 0) + 1);
      if (tx.to)   addressCounts.set(tx.to,   (addressCounts.get(tx.to)   || 0) + 1);
    }

    const client = await this.dbPool.connect();
    try {
      await client.query('BEGIN');

      // Batch insert transactions
      await client.query(
        `INSERT INTO transactions (
           hash, block_number, block_hash, from_address, to_address,
           value, gas, gas_price, gas_used, status, input_data, nonce,
           contract_address
         ) VALUES ${txPlaceholders.join(',')}
         ON CONFLICT (hash) DO UPDATE SET
           contract_address = EXCLUDED.contract_address
         WHERE transactions.contract_address IS NULL`,
        txParams
      );

      // Batch insert logs (single round-trip for all logs in the batch)
      if (allLogs.length > 0) {
        const logPlaceholders = [];
        const logParams = [];
        for (let i = 0; i < allLogs.length; i++) {
          const l = allLogs[i];
          const o = i * 6;
          logPlaceholders.push(`($${o+1},$${o+2},$${o+3},$${o+4},$${o+5},$${o+6})`);
          logParams.push(l.txHash, l.blockNumber, l.blockHash, l.index, l.address, l.data);
        }
        await client.query(
          `INSERT INTO logs (transaction_hash, block_number, block_hash, log_index, address, data)
           VALUES ${logPlaceholders.join(',')}
           ON CONFLICT (transaction_hash, log_index) DO NOTHING`,
          logParams
        );
      }

      // Batch upsert addresses — one row per unique address, count already summed
      if (addressCounts.size > 0) {
        const addrEntries    = [...addressCounts.entries()];
        const addrPlaceholders = [];
        const addrParams       = [];
        for (let i = 0; i < addrEntries.length; i++) {
          const [address, count] = addrEntries[i];
          const o = i * 2;
          addrPlaceholders.push(`($${o+1},$${o+2},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`);
          addrParams.push(address, count);
        }
        await client.query(
          `INSERT INTO addresses (address, transaction_count, first_seen, last_seen)
           VALUES ${addrPlaceholders.join(',')}
           ON CONFLICT (address) DO UPDATE SET
             transaction_count = addresses.transaction_count + EXCLUDED.transaction_count,
             last_seen = CURRENT_TIMESTAMP`,
          addrParams
        );
      }

      await client.query('COMMIT');

      this.lastTxId = parsed[parsed.length - 1].id;
      this.stats.transactionsProcessed += parsed.length;
      this.stats.logsProcessed         += allLogs.length;
      await this.redisClient.set('indexer:lastTxId', this.lastTxId);
      logger.info(`Indexed ${parsed.length} txs, ${allLogs.length} logs, ${addressCounts.size} addresses`);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  startHealthCheck() {
    const server = http.createServer((req, res) => {
      if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok', isRunning: this.isRunning, stats: this.stats }));
      } else if (req.url === '/stats') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(this.stats));
      } else {
        res.writeHead(404);
        res.end('Not found');
      }
    });

    server.listen(3101, () => {
      logger.info('Health check server listening on port 3101');
    });
  }

  async shutdown() {
    logger.info('Shutting down indexer...');
    this.isRunning = false;
    if (this.redisClient) {
      await this.redisClient.disconnect();
    }
    if (this.dbPool) {
      await this.dbPool.end();
    }
  }
}

// Main execution
async function main() {
  try {
    const indexer = new Indexer();
    await indexer.initialize();
    indexer.startHealthCheck();

    // Handle graceful shutdown
    process.on('SIGTERM', () => indexer.shutdown());
    process.on('SIGINT', () => indexer.shutdown());

    // Start indexing
    await indexer.startIndexing();
  } catch (error) {
    logger.error('Fatal error:', error);
    process.exit(1);
  }
}

main();
