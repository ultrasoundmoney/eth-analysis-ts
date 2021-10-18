import * as EthNode from "./eth_node.js";
import * as Duration from "./duration.js";
import * as DerivedBlockStats from "./derived_block_stats.js";
import * as Log from "./log.js";
import * as ContractsMetadata from "./contracts_metadata.js";
import { sql } from "./db.js";
import { delay } from "./delay.js";

const main = async () => {
  try {
    await EthNode.connect();
    Log.info("starting add-contract-metadata");

    let lastSeenStats = await DerivedBlockStats.getLatestDerivedBlockStats()();

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const latestStats =
        await DerivedBlockStats.getLatestDerivedBlockStats()();
      if (lastSeenStats.blockNumber === latestStats.blockNumber) {
        // Already added these stats to the queue.
        await delay(Duration.milisFromSeconds(1));
        continue;
      }

      await ContractsMetadata.addMetadataForLeaderboards(
        latestStats.leaderboards,
      )();

      lastSeenStats = latestStats;
    }
  } catch (error) {
    Log.error("error adding metadata", { error });
    EthNode.closeConnection();
    sql.end();
    throw error;
  }
};

main();

process.on("unhandledRejection", (error) => {
  throw error;
});
