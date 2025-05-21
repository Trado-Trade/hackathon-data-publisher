// Configuration for the application
import dotenv from "dotenv";
dotenv.config();
export const config = {
  mqtt: {
    host: "emqx.trado.trade",
    port: 8883,
    username: "hack_iitrpr",
    password: "hack_iitrpr",
    clientId: `hackathon-client-${Math.random().toString(16).substring(2, 8)}`,
  },
  db: {
    host: process.env.PGHOST || "localhost",
    port: parseInt(process.env.PGPORT || "5432"),
    user: process.env.PGUSER || "postgres",
    password: process.env.PGPASSWORD || "postgres",
    database: process.env.PGDATABASE || "market_data",
  },
  app: {
    indexPrefix: process.env.INDEX_PREFIX || "index",
    batchSize: parseInt(process.env.BATCH_SIZE || "100"),
    batchInterval: parseInt(process.env.BATCH_INTERVAL || "5000"), // 5 seconds
  },
};

// List of indices to track
export const INDICES = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"];

// Expiry dates for each index
export const EXPIRY_DATES = {
  NIFTY: "22-05-2025",
  BANKNIFTY: "29-05-2025",
  FINNIFTY: "29-05-2025",
  MIDCPNIFTY: "29-05-2025",
};

// Number of strikes above and below ATM to subscribe to
export const STRIKE_RANGE = 5;
