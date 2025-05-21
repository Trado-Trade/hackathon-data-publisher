import { Pool } from "pg";
import { config } from "../config";

// const pool = new Pool(config.db);
// Define a type for batch items
export interface BatchItem {
  topic: string;
  ltp: number;
  indexName?: string;
  type?: string;
  strike?: number;
}

// Initialize database connection pool
let pool: Pool;
let dataBatch: BatchItem[] = [];
let batchTimer: NodeJS.Timeout | null = null;

// Cache topic IDs to avoid repeated lookups
const topicCache = new Map<string, number>();

export function createPool(): Pool {
  return new Pool({
    host: config.db.host,
    port: config.db.port,
    user: config.db.user,
    password: config.db.password,
    database: config.db.database,
    ssl: false,
  });
}

export async function initialize(dbPool: Pool) {
  pool = dbPool;
  console.log("Database initialized");

  // TODO: Preload topic cache from database
  const res = await pool.query(`SELECT topic_id, topic_name FROM topics`);
  res.rows.forEach((row) => {
    topicCache.set(row.topic_name, row.topic_id);
  });
}

export async function getTopicId(
  topicName: string,
  indexName?: string,
  type?: string,
  strike?: number
): Promise<number> {
  // TODO: Implement this function
  // 1. Check if topic exists in cache
  // 2. If not in cache, check if it exists in database
  // 3. If not in database, insert it
  // 4. Return topic_id
  if (topicCache.has(topicName)) {
    return topicCache.get(topicName)!;
  }

  const existing = await pool.query(
    `SELECT topic_id FROM topics WHERE topic_name = $1`,
    [topicName]
  );

  if (existing.rows.length > 0) {
    const topic_id = existing.rows[0].topic_id;
    topicCache.set(topicName, topic_id);
    return topic_id;
  }

  const insert = await pool.query(
    `INSERT INTO topics (topic_name, index_name, type, strike)
    VALUES ($1, $2, $3, $4)
    RETURNING topic_id`,
    [topicName, indexName, type, strike]
  );

  const newId = insert.rows[0].topic_id;
  topicCache.set(topicName, newId);
  return newId;
}

export function saveToDatabase(
  topic: string,
  ltp: number,
  indexName?: string,
  type?: string,
  strike?: number
) {
  // TODO: Implement this function
  // 1. Add item to batch
  // 2. If batch timer is not running, start it
  // 3. If batch size reaches threshold, flush batch
  dataBatch.push({ topic, ltp, indexName, type, strike });

  if (!batchTimer) {
    batchTimer = setTimeout(() => flushBatch(), config.app.batchInterval);
  }

  if (dataBatch.length >= config.app.batchSize) {
    flushBatch();
  }

  console.log(`Saving to database: ${topic}, LTP: ${ltp}`);
}

export async function flushBatch() {
  // TODO: Implement this function
  // 1. Clear timer
  // 2. If batch is empty, return
  // 3. Process batch items (get topic IDs)
  // 4. Insert data in a transaction
  // 5. Reset batch
  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }

  if (dataBatch.length === 0) return;

  const batch = [...dataBatch];
  dataBatch = [];

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    for (const item of batch) {
      if (item.type && !Number.isFinite(item.strike)) {
        console.warn(`Skipping invalid strike topic: ${item.topic}`);
        continue;
      }
      const topicId = await getTopicId(
        item.topic,
        item.indexName,
        item.type,
        item.strike
      );

      await client.query(
        `INSERT INTO ltp_data (topic_id, ltp, received_at)
         VALUES ($1, $2, NOW())`,
        [topicId, item.ltp]
      );
    }

    await client.query("COMMIT");
    console.log(`Flushed ${batch.length} records`);
  } catch (err) {
    console.error("Error flushing batch:", err);
    await client.query("ROLLBACK");
  } finally {
    client.release();
  }

  console.log("Flushing batch to database");
}

export async function cleanupDatabase() {
  // Flush any remaining items in the batch
  if (dataBatch.length > 0) {
    await flushBatch();
  }

  // Close the database pool
  if (pool) {
    await pool.end();
  }

  console.log("Database cleanup completed");
}
