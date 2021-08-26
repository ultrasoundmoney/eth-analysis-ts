import Sentry from "@sentry/node";
import "@sentry/tracing";
import * as BaseFees from "./base_fees.js";
import * as Log from "./log.js";
import Config, { setName } from "./config.js";
import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";

// meaningless comment to trigger build

Sentry.init({
  dsn: "https://f6393dc2e2984ec09299406e8f409647@o920717.ingest.sentry.io/5896630",
  tracesSampleRate: 0.1,
  environment: Config.env,
});

setName("watch-base-fees");

BaseFees.watchAndCalcBaseFees().catch((error) => {
  Log.error("error watching and analyzing new blocks", { error });
  Sentry.captureException(error);
  EthNode.closeConnection();
  sql.end();
  throw error;
});
