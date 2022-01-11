import * as ContractsMetadata from "../contracts/crawl_metadata.js";
import { sql } from "../db.js";
import * as EthNode from "../eth_node.js";
import * as Log from "../log.js";

const main = async () => {
  try {
    Log.info("starting add-contract-metadata");
    await EthNode.connect();

    await ContractsMetadata.addMetadataForLeaderboards(
      ["0x59c0e4b889f4c036dd0d6d759c7b37cf91f3ec01"],
      new Set(),
    )();
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
