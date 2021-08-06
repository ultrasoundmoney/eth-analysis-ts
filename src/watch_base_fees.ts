import Sentry from "@sentry/node";
import "@sentry/tracing";
import * as BaseFees from "./base_fees.js";
import * as Log from "./log.js";
import { sql } from "./db.js";
import * as eth from "./web3.js";

Sentry.init({
  dsn: "https://f6393dc2e2984ec09299406e8f409647@o920717.ingest.sentry.io/5896630",
  tracesSampleRate: 0.1,
});

BaseFees.watchAndCalcBaseFees()
  .then(async () => {
    Log.info("done analyzing gas");
    eth.closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("error analyzing gas", { error });
    Sentry.captureException(error);
    throw error;
  });
