import { setName } from "./config.js";
setName("reanalyze-all-blocks");
import * as BaseFees from "./base_fees.js";
import * as Log from "./log.js";
import { sql } from "./db.js";
import * as Eth from "./web3.js";

BaseFees.reanalyzeAllBlocks()
  .then(async () => {
    Log.info("done reanalyzing all blocks");
    Eth.closeWeb3Ws();
    sql.end();
  })
  .catch((error) => {
    Log.error("error reanalyzing all blocks", { error });
    Eth.closeWeb3Ws();
    sql.end();
    throw error;
  });
