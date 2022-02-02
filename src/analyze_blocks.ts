import * as BlocksNewBlock from "./blocks/new_head.js";
import * as BlocksSync from "./blocks/sync.js";
import * as BurnRecordsSync from "./burn-records/sync.js";
import * as Config from "./config.js";
import { runMigrations, sql } from "./db.js";
import * as EthNode from "./eth_node.js";
import { TAlt } from "./fp.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as Performance from "./performance.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as EthLocked from "./scarcity/eth_locked.js";
import * as EthStaked from "./scarcity/eth_staked.js";
import * as EthSupply from "./scarcity/eth_supply.js";

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
  await runMigrations();

  const chainHeadOnStart = await EthNode.getLatestBlockNumber();
  Log.debug(`fast-sync blocks up to ${chainHeadOnStart}`);
  EthNode.subscribeNewHeads(BlocksNewBlock.onNewBlock);
  Log.debug("listening and queuing new chain heads for analysis");
  await BlocksSync.syncBlocks(chainHeadOnStart);
  Log.info("fast-sync blocks done");

  await TAlt.seqTParT(
    Performance.measureTaskPerf("sync burn records", BurnRecordsSync.sync()),
    EthLocked.init(),
    () => EthStaked.init(),
    () => EthSupply.init(),
    Performance.measureTaskPerf("init leaderboard limited timeframes", () =>
      initLeaderboardLimitedTimeframes(),
    ),
    Performance.measureTaskPerf("init leaderboard all", () =>
      syncLeaderboardAll(),
    ),
  )();

  BlocksNewBlock.newBlockQueue.start();
  Log.info("started analyzing new blocks from queue");
} catch (error) {
  EthNode.closeConnection();
  sql.end();
  throw error;
}
