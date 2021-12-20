import Sentry from "@sentry/node";
import "@sentry/tracing";
import * as BlocksNewBlock from "./blocks/new_head.js";
import * as BlocksSync from "./blocks/sync.js";
import * as Config from "./config.js";
import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";
// import * as BurnRecordsSync from "./burn-records/sync.js";
import * as FeeBurns from "./fee_burns.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as EthLocked from "./scarcity/eth_locked.js";
import * as EthStaked from "./scarcity/eth_staked.js";
import * as EthSupply from "./scarcity/eth_supply.js";

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
  await LeaderboardsAll.addMissingBlocks();
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

  const chainHeadOnStart = await EthNode.getLatestBlockNumber();
  Log.debug(`fast-sync blocks up to ${chainHeadOnStart}`);
  EthNode.subscribeNewHeads(BlocksNewBlock.onNewBlock);
  Log.debug("listening and queuing new chain heads for analysis");
  await BlocksSync.syncBlocks(chainHeadOnStart);
  Log.info("fast-sync blocks done");

  await Promise.all([
    initLeaderboardLimitedTimeframes(),
    // BurnRecordsSync.init(),
    syncLeaderboardAll(),
    FeeBurns.init()(),
    EthLocked.init(),
    EthStaked.init(),
    EthSupply.init(),
  ]);

  BlocksNewBlock.newBlockQueue.start();
  Log.info("started analyzing new blocks from queue");
} catch (error) {
  EthNode.closeConnection();
  sql.end();
  throw error;
}
