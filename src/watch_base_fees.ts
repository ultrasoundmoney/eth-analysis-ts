import * as BaseFees from "./base_fees.js";
import * as Log from "./log.js";
import { sql } from "./db.js";
import * as eth from "./web3.js";

BaseFees.watchAndCalcBaseFees()
  .then(async () => {
    Log.info("done analyzing gas");
    eth.closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("error analyzing gas", { error });
    throw error;
  });
