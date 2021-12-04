import Sentry from "@sentry/node";
import "@sentry/tracing";
import * as Config from "./config.js";
import * as StoreNewBlock from "./blocks/store_new_block.js";
import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import { syncBlocks } from "./blocks/sync.js";
import { newBlockQueue } from "./blocks/store_new_block.js";

process.on("unhandledRejection", (error) => {
  throw error;
});

if (Config.getEnv() !== "dev") {
  Sentry.init({
    dsn: "https://f6393dc2e2984ec09299406e8f409647@o920717.ingest.sentry.io/5896630",
    tracesSampleRate: 0.1,
    environment: Config.getEnv(),
  });
}

PerformanceMetrics.setShouldLogBlockFetchRate(true);

const syncLeaderboardAll = async (): Promise<void> => {
  Log.info("adding missing blocks to leaderboard all");
  await LeaderboardsAll.addMissingBlocks()();
  Log.info("done adding missing blocks to leaderboard all");
};

const initLeaderboardLimitedTimeframes = async (): Promise<void> => {
  Log.info("loading leaderboards for limited timeframes");
  await LeaderboardsLimitedTimeframe.addAllBlocksForAllTimeframes()();
  Log.info("done loading leaderboards for limited timeframes");
};

try {
  Config.ensureCriticalBlockAnalysisConfig();
  await EthNode.connect();
  Log.debug("started processing new blocks");

  EthNode.subscribeNewHeads((head) =>
    newBlockQueue.add(StoreNewBlock.storeNewBlock(head.number)),
  );
  Log.info("listening for and queueing new blocks to add");
  await syncBlocks()();
  Log.info("done adding missing blocks");

  await Promise.all([
    initLeaderboardLimitedTimeframes(),
    // BurnRecordsAllSync.sync,
    syncLeaderboardAll(),
  ]);

  newBlockQueue.start();
  Log.info("started analyzing new blocks from queue");
} catch (error) {
  Log.error("error adding new blocks", { error });
  EthNode.closeConnection();
  sql.end();
  throw error;
}
