import * as BaseFees from "./base_fees.js";
import * as Log from "./log.js";
import { sql } from "./db.js";
import * as eth from "./web3.js";

BaseFees.reanalyzeAllBlocks()
  .then(async () => {
    Log.info("done reanalyzing all blocks");
    eth.closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("error reanalyzing all blocks", { error });
    throw error;
  });
