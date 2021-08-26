import Sentry from "@sentry/node";
import "@sentry/tracing";
import * as BaseFeeTotals from "./base_fee_totals.js";
import * as Log from "./log.js";
import Config, { setName } from "./config.js";
import * as EthNode from "./eth_node.js";
import { sql } from "./db.js";

Sentry.init({
  dsn: "https://bb2017e4c0cc48649fcda8115eebd113@o920717.ingest.sentry.io/5896651",
  tracesSampleRate: 0.1,
  environment: Config.env,
});

setName("watch-base-fee-totals");

BaseFeeTotals.watchAndCalcTotalFees().catch((error) => {
  Log.error("error watching and analyzing for new base fee totals", {
    error,
  });
  Sentry.captureException(error);
  EthNode.closeConnection();
  sql.end();
  throw error;
});
