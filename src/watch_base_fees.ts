import Sentry from "@sentry/node";
import "@sentry/tracing";
import Config from "./config.js";
import * as BaseFees from "./base_fees.js";
import * as Log from "./log.js";
import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";

if (Config.env !== "dev") {
  Sentry.init({
    dsn: "https://f6393dc2e2984ec09299406e8f409647@o920717.ingest.sentry.io/5896630",
    tracesSampleRate: 0.1,
    environment: Config.env,
  });
}

const main = async () => {
  try {
    Log.info("watching and analyzing new blocks");
    await EthNode.connect();
    BaseFees.watchAndCalcBaseFees();
  } catch (error) {
    Log.error("error watching and analyzing new blocks", { error });
    throw error;
  } finally {
    EthNode.closeConnection();
    sql.end();
  }
};

main();

process.on("unhandledRejection", (error) => {
  throw error;
});
