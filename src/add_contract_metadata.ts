import * as Contracts from "./contracts.js";
import * as ContractsMetadata from "./contracts_metadata.js";
import { sql } from "./db.js";
import { delay } from "./delay.js";
import * as DerivedBlockStats from "./derived_block_stats.js";
import * as Duration from "./duration.js";
import * as EthNode from "./eth_node.js";
import { pipe } from "./fp.js";
import * as Log from "./log.js";

const main = async () => {
  Log.info("starting add-contract-metadata");
  try {
    await EthNode.connect();

    let lastSeenStats = await DerivedBlockStats.getLatestDerivedBlockStats()();

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const latestStats =
        await DerivedBlockStats.getLatestDerivedBlockStats()();
      if (lastSeenStats.blockNumber === latestStats.blockNumber) {
        // Already added these stats to the queue.
        Log.debug(
          `already added metadata for block ${latestStats.blockNumber}, waiting and checking for new leaderboard`,
        );
        await delay(Duration.milisFromSeconds(1));
        continue;
      }

      const addresses = pipe(
        latestStats.leaderboards,
        ContractsMetadata.getAddressesForMetadata,
        (set) => Array.from(set),
      );

      Log.debug(
        `adding metadata for ${addresses.length} addresses in leaderboard for block ${latestStats.blockNumber}`,
      );

      await ContractsMetadata.addMetadataForLeaderboards(addresses)();
      await Contracts.setLastLeaderboardEntryToNow(addresses);

      lastSeenStats = latestStats;

      Log.info(
        `done adding metadata for leaderboard of block: ${latestStats.blockNumber}`,
      );
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
