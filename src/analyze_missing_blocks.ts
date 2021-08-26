import * as BaseFees from "./base_fees.js";
import { sql } from "./db.js";
import * as Log from "./log.js";
import * as EthNode from "./eth_node.js";

const main = async () => {
  try {
    await EthNode.connect();
    await BaseFees.analyzeMissingBlocks();
  } catch (error) {
    Log.error("error watching and analyzing new blocks", { error });
    throw error;
  } finally {
    EthNode.closeConnection();
    await sql.end();
  }
};

main();

process.on("unhandledRejection", (error) => {
  throw error;
});
