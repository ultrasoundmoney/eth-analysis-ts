import * as BaseFees from "./base_fees.js";
import * as Log from "./log.js";
import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";

const main = async () => {
  try {
    Log.info("reanalyzing all blocks");
    await EthNode.connect();
    await BaseFees.reanalyzeAllBlocks();
    Log.info("done reanalyzing all blocks");
  } catch (error) {
    Log.error("error reanalyzing all blocks", { error });
    throw error;
  } finally {
    EthNode.closeConnection();
    sql.end();
  }
};

main();
