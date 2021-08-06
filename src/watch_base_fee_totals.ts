import Sentry from "@sentry/node";
import "@sentry/tracing";
import * as BaseFeeTotals from "./base_fee_totals.js";
import * as eth from "./web3.js";
import * as Log from "./log.js";
import { sql } from "./db.js";

Sentry.init({
  dsn: "https://bb2017e4c0cc48649fcda8115eebd113@o920717.ingest.sentry.io/5896651",
  tracesSampleRate: 0.1,
});

BaseFeeTotals.watchAndCalcTotalFees()
  .then(async () => {
    Log.info("done analyzing blocks for base fee totals");
    eth.closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("error analyzing base fee totals", { error });
    Sentry.captureException(error);
    throw error;
  });
