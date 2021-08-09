import Sentry from "@sentry/node";
import "@sentry/tracing";
import * as BaseFeeTotals from "./base_fee_totals.js";
import * as Log from "./log.js";
import Config from "./config.js";

Sentry.init({
  dsn: "https://bb2017e4c0cc48649fcda8115eebd113@o920717.ingest.sentry.io/5896651",
  tracesSampleRate: 0.1,
  environment: Config.env,
});

BaseFeeTotals.watchAndCalcTotalFees().catch((error) => {
  Log.error("error watching and analyzing for new base fee totals", {
    error,
  });
  Sentry.captureException(error);
  throw error;
});
