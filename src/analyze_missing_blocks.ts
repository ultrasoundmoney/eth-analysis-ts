import * as BaseFees from "./base_fees.js";
import { setName } from "./config.js";
import { sql } from "./db.js";
import * as Log from "./log.js";
import * as Eth from "./web3.js";

setName("analyze-missing-blocks");

BaseFees.analyzeMissingBlocks()
  .then(async () => {
    Eth.closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("error watching and analyzing new blocks", { error });
    throw error;
  });
