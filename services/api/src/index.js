import express from 'express';
import cors from 'cors';
import http from 'http';
import { WebSocketServer } from 'ws';
import pg from 'pg';
import { createClient } from 'redis';
import pino from 'pino';
import pinoHttp from 'pino-http';
import dotenv from 'dotenv';

import blocksRouter from './routes/blocks.js';
import transactionsRouter from './routes/transactions.js';
import addressesRouter from './routes/addresses.js';

dotenv.config();

const logger = pino({ level: process.env.LOG_LEVEL || 'info' });
const { Pool } = pg;

class API {
  constructor() {
    this.app = express();
    this.httpServer = http.createServer(this.app);
    this.wss = null;
    this.dbPool = null;
    this.redisClient = null;    // general purpose client
    this.subClient   = null;    // dedicated to XREAD BLOCK (blocking reads)
    this.port = process.env.PORT || 3000;
    this.connectedClients = new Set();
  }

  async initialize() {
    try {
      // Initialize Database
      this.dbPool = new Pool({ connectionString: process.env.DATABASE_URL });
      const client = await this.dbPool.connect();
      await client.query('SELECT 1');
      client.release();
      logger.info('Database connected');

      // Initialize Redis (two clients: one general, one for blocking stream reads)
      this.redisClient = createClient({ url: process.env.REDIS_URL });
      this.redisClient.on('error', (err) => logger.error('Redis error:', err));
      await this.redisClient.connect();

      this.subClient = this.redisClient.duplicate();
      this.subClient.on('error', (err) => logger.error('Redis sub error:', err));
      await this.subClient.connect();
      logger.info('Redis connected');

      // Setup Express middleware
      this.app.use(cors());
      this.app.use(pinoHttp({ logger }));
      this.app.use(express.json());

      // Setup routes
      this.setupRoutes();

      // Setup WebSocket
      this.setupWebSocket();

      // Start Redis → WebSocket broadcaster
      this.startStreamListener();

      // Setup error handling
      this.app.use((err, req, res, next) => {
        logger.error(err);
        res.status(err.status || 500).json({ error: err.message });
      });
    } catch (error) {
      logger.error({ err: error }, 'Initialization error');
      throw error;
    }
  }

  setupRoutes() {
    this.app.get('/health', (req, res) => {
      res.json({ status: 'ok', timestamp: new Date().toISOString(), connectedClients: this.connectedClients.size });
    });

    // Keep REST stats for backward compat
    this.app.get('/stats', async (req, res) => {
      try { res.json(await this.getStats()); }
      catch (error) { res.status(500).json({ error: error.message }); }
    });

    this.app.use('/api/blocks',       blocksRouter(this.dbPool));
    this.app.use('/api/transactions', transactionsRouter(this.dbPool));
    this.app.use('/api/addresses',    addressesRouter(this.dbPool));

    logger.info('Routes configured');
  }

  setupWebSocket() {
    this.wss = new WebSocketServer({ server: this.httpServer });

    this.wss.on('connection', async (ws) => {
      logger.info('New WebSocket connection');
      this.connectedClients.add(ws);

      ws.on('message', (raw) => {
        try {
          const msg = JSON.parse(raw);
          if (msg.type === 'ping') ws.send(JSON.stringify({ type: 'pong' }));
        } catch {}
      });

      ws.on('close', () => {
        this.connectedClients.delete(ws);
      });
      ws.on('error', () => ws.close());

      // Send current state immediately on connect
      try {
        const [stats, blocksRes, txsRes] = await Promise.all([
          this.getStats(),
          this.dbPool.query(
            `SELECT hash, number, timestamp, miner, transactions_count, gas_used
             FROM blocks ORDER BY number DESC LIMIT 20`
          ),
          this.dbPool.query(
            `SELECT hash, block_number, from_address, to_address, value, status
             FROM transactions ORDER BY id DESC LIMIT 20`
          ),
        ]);
        ws.send(JSON.stringify({
          type: 'init',
          data: { stats, blocks: blocksRes.rows, txs: txsRes.rows },
        }));
      } catch (err) {
        logger.error('init send error:', err.message);
      }
    });

    logger.info('WebSocket server configured');
  }

  // Broadcast a message to all connected WebSocket clients
  broadcast(msg) {
    if (this.connectedClients.size === 0) return;
    const str = JSON.stringify(msg);
    for (const ws of this.connectedClients) {
      if (ws.readyState === 1 /* OPEN */) ws.send(str);
    }
  }

  // Background loop: subscribe to Redis streams and push events to WS clients
  startStreamListener() {
    let lastBlockId = '$'; // only new entries from now
    let lastTxId    = '$';

    const loop = async () => {
      while (true) {
        try {
          const result = await this.subClient.xRead(
            [
              { key: 'blocks:stream',       id: lastBlockId },
              { key: 'transactions:stream', id: lastTxId    },
            ],
            { COUNT: 1000 }
          );

          if (!result) {
            await new Promise((r) => setTimeout(r, 100));
            continue;
          }

          let hasNewBlocks = false;

          for (const stream of result) {
            if (stream.name === 'blocks:stream') {
              for (const { id, message } of stream.messages) {
                lastBlockId = id;
                try {
                  const b = JSON.parse(message.blockData);
                  this.broadcast({
                    type: 'block',
                    data: {
                      hash:               b.hash,
                      number:             b.number,
                      timestamp:          b.timestamp,
                      miner:              b.miner,
                      gas_used:           b.gasUsed,
                      transactions_count: b.transactions?.length ?? 0,
                    },
                  });
                  hasNewBlocks = true;
                } catch {}
              }
            }

            if (stream.name === 'transactions:stream') {
              for (const { id, message } of stream.messages) {
                lastTxId = id;
                try {
                  const tx = JSON.parse(message.txData);
                  this.broadcast({
                    type: 'tx',
                    data: {
                      hash:         tx.hash,
                      block_number: tx.blockNumber,
                      from_address: tx.from,
                      to_address:   tx.to,
                      value:        tx.value,
                      status:       tx.status,
                    },
                  });
                } catch {}
              }
            }
          }

          // Push fresh stats after each block batch
          if (hasNewBlocks) {
            try {
              const stats = await this.getStats();
              this.broadcast({ type: 'stats', data: stats });
            } catch {}
          }

          // Poll again immediately if we got data; otherwise sleep in the !result branch
        } catch (err) {
          logger.error('Stream listener error:', err.message);
          await new Promise((r) => setTimeout(r, 1000));
        }
      }
    };

    loop().catch((err) => logger.error('Stream listener fatal:', err.message));
    logger.info('Stream listener started (broadcasting blocks + txs to WS clients)');

    // Periodic stats heartbeat — updates the metrics bar even when no new blocks arrive
    setInterval(async () => {
      try {
        const stats = await this.getStats();
        this.broadcast({ type: 'stats', data: stats });
      } catch {}
    }, 5000);
  }

  async getStats() {
    const [statsRes, rangeRes, tpsRes] = await Promise.all([
      this.dbPool.query(`
        SELECT
          (SELECT COUNT(*)          FROM blocks)::text        AS total_blocks,
          (SELECT MAX(number)::text  FROM blocks)             AS block_height,
          (SELECT MAX(timestamp)::text FROM blocks)           AS latest_block_timestamp,
          (SELECT COUNT(*)          FROM transactions)::text  AS total_transactions,
          (SELECT COUNT(*)          FROM addresses)::text     AS total_addresses,
          (SELECT COUNT(*)          FROM logs)::text          AS total_logs
      `),
      // avg_block_time: unchanged — 101 blocks for a stable average
      this.dbPool.query(`
        SELECT
          (MAX(number)::bigint    - MIN(number)::bigint)    AS block_count,
          (MAX(timestamp)::bigint - MIN(timestamp)::bigint) AS time_span
        FROM (
          SELECT number, timestamp::bigint
          FROM blocks ORDER BY number DESC LIMIT 101
        ) t
      `),
      // TPS: latest block txns ÷ time between last 2 blocks
      this.dbPool.query(`
        SELECT
          (MAX(number)::bigint    - MIN(number)::bigint)    AS block_count,
          (MAX(timestamp)::bigint - MIN(timestamp)::bigint) AS time_span,
          (SELECT transactions_count FROM blocks ORDER BY number DESC LIMIT 1) AS latest_txns
        FROM (
          SELECT number, timestamp::bigint
          FROM blocks ORDER BY number DESC LIMIT 20
        ) t
      `),
    ]);

    let avg_block_time = null;
    const { block_count, time_span } = rangeRes.rows[0];
    if (block_count && parseInt(block_count) > 0 && time_span && parseInt(time_span) > 0) {
      avg_block_time = (parseInt(time_span) / parseInt(block_count)).toFixed(3);
    }

    let tps = '0.00';
    const tr = tpsRes.rows[0];
    if (tr.block_count && parseInt(tr.block_count) > 0 && tr.time_span && parseInt(tr.time_span) > 0) {
      const block_time = parseInt(tr.time_span) / parseInt(tr.block_count);
      tps = (parseInt(tr.latest_txns || 0) / block_time).toFixed(2);
    }

    return { ...statsRes.rows[0], avg_block_time, tps };
  }

  start() {
    this.httpServer.listen(this.port, () => {
      logger.info(`API server listening on port ${this.port}`);
    });
  }

  async shutdown() {
    logger.info('Shutting down API server...');
    this.connectedClients.forEach((c) => c.close());
    if (this.wss)         this.wss.close();
    this.httpServer.close();
    if (this.subClient)   await this.subClient.disconnect();
    if (this.redisClient) await this.redisClient.disconnect();
    if (this.dbPool)      await this.dbPool.end();
  }
}

// Main execution
async function main() {
  try {
    const api = new API();
    await api.initialize();
    api.start();

    // Handle graceful shutdown
    process.on('SIGTERM', () => api.shutdown());
    process.on('SIGINT', () => api.shutdown());
  } catch (error) {
    logger.error({ err: error }, 'Fatal error');
    process.exit(1);
  }
}

main();
