import * as Contracts from "./contracts.js";
import * as ContractsMetadata from "./contracts_metadata.js";
import { sql } from "./db.js";
import { delay } from "./delay.js";
import * as DerivedBlockStats from "./derived_block_stats.js";
import * as Duration from "./duration.js";
import * as EthNode from "./eth_node.js";
import { pipe, TEAlt } from "./fp.js";
import * as Log from "./log.js";

const main = async () => {
  Log.info("starting add-contract-metadata");
  try {
    await EthNode.connect();

    let lastSeenStats = await pipe(
      DerivedBlockStats.getLatestLeaderboards(),
      TEAlt.getOrThrow,
    )();

    // eslint-disable-next-line no-constant-condition
    while (true) {
      const latestStats = await pipe(
        DerivedBlockStats.getLatestLeaderboards(),
        TEAlt.getOrThrow,
      )();

      if (lastSeenStats.blockNumber === latestStats.blockNumber) {
        // Already added these stats to the queue.
        Log.debug(
          `already added metadata for block ${latestStats.blockNumber}, waiting and checking for new leaderboard`,
        );
        await delay(Duration.millisFromSeconds(1));
        continue;
      }

      const addressesToRefetch = await Contracts.getAddressesToRefetch()();
      const addresses = pipe(
        latestStats.leaderboards,
        ContractsMetadata.getAddressesForMetadata,
        // Make sure contracts we want to refetch are fetched.
        (leaderboardAddresses) => [
          ...leaderboardAddresses,
          ...addressesToRefetch,
        ],
        (set) => Array.from(set),
      );

      Log.debug(
        `adding metadata for ${addresses.length} addresses in leaderboard for block ${latestStats.blockNumber}`,
      );

      await ContractsMetadata.addMetadataForLeaderboards(
        addresses,
        addressesToRefetch,
      )();
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
