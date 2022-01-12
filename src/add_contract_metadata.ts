import { setInterval } from "timers/promises";
import * as Contracts from "./contracts/contracts.js";
import * as ContractsMetadata from "./contracts/crawl_metadata.js";
import * as Duration from "./duration.js";
import * as EthNode from "./eth_node.js";
import { pipe, T } from "./fp.js";
import * as GroupedAnalysis1 from "./grouped_analysis_1.js";
import * as Log from "./log.js";

process.on("unhandledRejection", (error) => {
  throw error;
});

const intervalIterator = setInterval(Duration.millisFromSeconds(4), Date.now());

await EthNode.connect();

let lastAnalyzed = await pipe(
  GroupedAnalysis1.getLatestLeaderboards(),
  T.map((stats) => stats.number),
)();

// eslint-disable-next-line @typescript-eslint/no-unused-vars
for await (const _ of intervalIterator) {
  const latestStats = await GroupedAnalysis1.getLatestLeaderboards()();

  if (lastAnalyzed === latestStats.number) {
    Log.debug(
      `leaderboard for block ${latestStats.number} already analyzed, waiting`,
    );
    continue;
  }

  const addressesToRefetch = await Contracts.getAddressesToRefetch()();
  const addresses = pipe(
    latestStats.leaderboards,
    ContractsMetadata.getAddressesForMetadata,
    // Make sure contracts we want to refetch are fetched.
    (leaderboardAddresses) => [...leaderboardAddresses, ...addressesToRefetch],
    (set) => Array.from(set),
  );

  Log.debug(
    `adding metadata for ${addresses.length} addresses in leaderboard for block ${latestStats.number}`,
  );

  await ContractsMetadata.addMetadataForLeaderboards(
    addresses,
    addressesToRefetch,
  )();
  await Contracts.setLastLeaderboardEntryToNow(addresses);

  lastAnalyzed = latestStats.number;

  Log.info(
    `done adding metadata for leaderboard of block: ${latestStats.number}`,
  );
}
