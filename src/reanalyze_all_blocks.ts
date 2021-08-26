import * as BaseFees from "./base_fees.js";
import * as Log from "./log.js";
import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";

BaseFees.reanalyzeAllBlocks()
  .then(async () => {
    Log.info("done reanalyzing all blocks");
    EthNode.closeConnection();
    sql.end();
  })
  .catch((error) => {
    Log.error("error reanalyzing all blocks", { error });
    throw error;
  });
