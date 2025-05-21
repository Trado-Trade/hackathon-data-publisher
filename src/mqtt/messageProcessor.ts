import mqtt from "mqtt";
import * as marketdata from "../proto/market_data_pb";
import * as subscriptionManager from "./subscriptionManager";
import * as db from "../db";
import * as utils from "../utils";

// Store LTP values for indices
const indexLtpMap = new Map<string, number>();
const atmStrikeMap = new Map<string, number>();

export async function processMessage(
  topic: string,
  message: Buffer,
  client: mqtt.MqttClient
) {
  try {
    // TODO: Implement this function
    // 1. Parse the message (it's likely in JSON format)
    // 2. Extract LTP value
    // 3. If it's an index topic, calculate ATM and subscribe to options (This is one time operation only)
    // 4. Save data to database

    // Decoding logic
    let decoded: any = null;
    let ltpValues: number[] = [];

    try {
      // Try decoding as MarketData
      if (message.length > 20) {
        decoded = marketdata.marketdata.MarketData.decode(
          new Uint8Array(message)
        );
        if (decoded?.ltp && typeof decoded.ltp === "number") {
          ltpValues.push(decoded.ltp);
        }
      }
    } catch (e1) {
      try {
        decoded = marketdata.marketdata.MarketDataBatch.decode(
          new Uint8Array(message)
        );
        if (decoded?.data?.length) {
          ltpValues = decoded.data
            .map((d: any) => d.ltp)
            .filter((v: any) => typeof v === "number");
        }
      } catch (e2) {
        try {
          const json = JSON.parse(message.toString());
          if (json?.ltp && typeof json.ltp === "number") {
            ltpValues.push(json.ltp);
          }
        } catch (e3) {
          console.warn(`Failed to decode message on topic ${topic}`);
          return;
        }
      }
    }

    // ltpValues now contains the decoded LTP values
    for (const ltp of ltpValues) {
      // Only run ATM logic for index topics
      if (topic.startsWith("index/")) {
        const index = topic.split("/")[1];

        if (!indexLtpMap.has(index)) {
          indexLtpMap.set(index, ltp);

          const atm =
            Math.round(ltp / utils.getStrikeDiff(index)) *
            utils.getStrikeDiff(index);
          atmStrikeMap.set(index, atm);

          console.log(`${index} ATM calculated: ${atm}`);
          await subscriptionManager.subscribeToAtmOptions(client, index, atm);
        }

        db.saveToDatabase(topic, ltp, index);
      }

      // Handle option topics like: "BANKNIFTY_55000_CE"
      else {
        const parts = topic.split("_");
        const index = parts[0];
        const strike = parseInt(parts[1]);
        const type = parts[2];

        if (!index || !type || !Number.isFinite(strike)) {
          console.warn(`Invalid option topic: ${topic}`);
          continue;
        }

        db.saveToDatabase(topic, ltp, index, type, strike);
      }
    }
  } catch (error) {
    console.error("Error processing message:", error);
  }
}
