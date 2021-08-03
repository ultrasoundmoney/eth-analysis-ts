import * as BaseFees from "./base_fees.js";
import * as Log from "./log.js";
import { sql } from "./db.js";
import { closeWeb3Ws } from "./web3.js";

// TODO: update implementation to analyze mainnet after fork block.

BaseFees.watchAndCalcBaseFees()
  .then(async () => {
    Log.info("> done analyzing gas");
    closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("> error analyzing gas", { error });
    throw error;
  });
