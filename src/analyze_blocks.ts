import * as Blocks from "./blocks/blocks.js";
import * as BlockLag from "./block_lag.js";
import * as BlocksNewBlock from "./blocks/new_head.js";
import * as BlocksSync from "./blocks/sync.js";
import * as BurnRecordsSync from "./burn-records/sync.js";
import * as Config from "./config.js";
import { runMigrations, sql } from "./db.js";
import * as ExecutionNode from "./execution_node.js";
import { pipe, T, TAlt } from "./fp.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as Performance from "./performance.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as EthLocked from "./scarcity/eth_locked.js";
import * as EthStaked from "./scarcity/eth_staked.js";
import * as EthSupply from "./scarcity/eth_supply.js";
import * as SyncOnStart from "./sync_on_start.js";

PerformanceMetrics.setShouldLogBlockFetchRate(true);

const syncLeaderboardAll = () =>
  pipe(
    Log.infoIO("adding missing blocks to leaderboard all"),
    T.fromIO,

    T.chain(() => () => LeaderboardsAll.addMissingBlocks()),
    T.chainFirstIOK(() =>
      Log.infoIO("done adding missing blocks to leaderboard all"),
    ),
  );

const initLeaderboardLimitedTimeframes = async (): Promise<void> => {
  Log.info("loading leaderboards for limited timeframes");
  await LeaderboardsLimitedTimeframe.addAllBlocksForAllTimeframes()();
  Log.info("done loading leaderboards for limited timeframes");
};

try {
  Config.ensureCriticalBlockAnalysisConfig();
  await runMigrations();

  const lastStoredBlockOnStart = await Blocks.getLastStoredBlock()();
  const chainHeadOnStart = await ExecutionNode.getLatestBlockNumber();
  Log.debug(`fast-sync blocks up to ${chainHeadOnStart}`);
  ExecutionNode.subscribeNewHeads(BlocksNewBlock.onNewBlock);
  Log.debug("listening and queuing new chain heads for analysis");
  await BlocksSync.syncBlocks(chainHeadOnStart);
  Log.info("fast-sync blocks done");

  await TAlt.seqTSeq(
    pipe(
      BurnRecordsSync.sync(),
      Performance.measureTaskPerf("sync burn records"),
    ),
    EthLocked.init(),
    () => EthStaked.init(),
    () => EthSupply.init(),
    pipe(
      () => initLeaderboardLimitedTimeframes(),
      Performance.measureTaskPerf("init leaderboard limited timeframes"),
    ),
    pipe(
      syncLeaderboardAll(),
      Performance.measureTaskPerf("init leaderboard all"),
    ),
    pipe(
      SyncOnStart.sync(lastStoredBlockOnStart.number + 1, chainHeadOnStart),
      Performance.measureTaskPerf("sync-next on start"),
    ),
    pipe(BlockLag.init, Performance.measureTaskPerf("init block lag")),
  )();

  BlocksNewBlock.headsQueue.start();
  Log.info("started analyzing new blocks from queue");
} catch (error) {
  ExecutionNode.closeConnections();
  sql.end();
  throw error;
}
