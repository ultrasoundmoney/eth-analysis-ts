import * as EthNode from "./eth_node.js";
import * as Log from "./log.js";
import * as ContractsMetadata from "./contracts_metadata.js";
import { sql } from "./db.js";

const main = async () => {
  try {
    Log.info("starting add-contract-metadata");
    await EthNode.connect();

    await ContractsMetadata.addMetadataForLeaderboards([
      "0x4fabb145d64652a948d72533023f6e7a623c7c53",
    ])();
  } catch (error) {
    Log.error("error adding metadata", { error });
  }

  EthNode.closeConnection();
  sql.end();
};

main();

process.on("unhandledRejection", (error) => {
  throw error;
});
