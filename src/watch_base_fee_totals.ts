import * as BaseFeeTotals from "./base_fee_totals.js";
import * as eth from "./web3.js";
import * as Log from "./log.js";
import { sql } from "./db.js";

BaseFeeTotals.watchAndCalcTotalFees()
  .then(async () => {
    Log.info("> done analyzing blocks for base fee totals");
    eth.closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("> error analyzing base fee totals", { error });
    throw error;
  });
