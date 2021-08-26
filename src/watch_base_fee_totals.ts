import Sentry from "@sentry/node";
import "@sentry/tracing";
import Config from "./config.js";
import * as BaseFeeTotals from "./base_fee_totals.js";
import * as Log from "./log.js";
import * as EthNode from "./eth_node.js";
import { sql } from "./db.js";

if (Config.env !== "dev") {
  Sentry.init({
    dsn: "https://bb2017e4c0cc48649fcda8115eebd113@o920717.ingest.sentry.io/5896651",
    tracesSampleRate: 0.1,
    environment: Config.env,
  });
}

BaseFeeTotals.watchAndCalcTotalFees().catch((error) => {
  Log.error("error watching and analyzing for new base fee totals", {
    error,
  });
  EthNode.closeConnection();
  sql.end();
  throw error;
});

process.on("unhandledRejection", (error) => {
  throw error;
});
