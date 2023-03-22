import Koa from "koa";
import * as Blocks from "./blocks/blocks.js";
import * as BlocksNewBlock from "./blocks/new_head.js";
import * as BlocksSync from "./blocks/sync.js";
import * as BlockLag from "./block_lag.js";
import * as BurnRecordsSync from "./burn-records/sync.js";
import * as Config from "./config.js";
import * as Db from "./db.js";
import * as ExecutionNode from "./execution_node.js";
import { ErrAlt, pipe, T, TE } from "./fp.js";
import * as LeaderboardsUnlimitedTimeframe from "./leaderboards_unlimited_time_frames.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as Performance from "./performance.js";
import * as EthStaked from "./scarcity/eth_staked.js";
import * as EthSupply from "./scarcity/eth_supply.js";
import * as SyncOnStart from "./sync_on_start.js";

const initLeaderboardLimitedTimeframes = async (): Promise<void> => {
  Log.info("loading leaderboards for limited timeframes");
  await LeaderboardsLimitedTimeframe.addAllBlocksForAllTimeframes()();
  Log.info("done loading leaderboards for limited timeframes");
};

const port = process.env.PORT || 3001;

const app = new Koa();

app.on("error", (err) => {
  Log.error("unhandled error", err);
});

// Health check middleware
app.use(async (ctx, next) => {
  if (
    ctx.path === "/healthz" ||
    ctx.path === "/health" ||
    ctx.path === "/api/fees/healthz"
  ) {
    await Db.checkHealth();
    await ExecutionNode.checkHealth();
    ctx.res.writeHead(200);
    ctx.res.end();
    return undefined;
  }

  await next();
  return undefined;
});

const startHealthCheckServer = async () => {
  await new Promise((resolve) => {
    app.listen(port, () => {
      resolve(undefined);
    });
  });

  Log.info(`listening on ${port}`);
};

const main = pipe(
  T.Do,
  T.apS(
    "_ensureCriticalConfig",
    T.fromIO(Config.ensureCriticalBlockAnalysisConfig),
  ),
  T.apS("_runMigrations", Db.runMigrations),
  T.apS("_startHealthCheckServer", startHealthCheckServer),
  T.apS("lastStoredBlockOnStart", () => Blocks.getLastStoredBlock()()),
  T.apS("chainHeadOnStart", ExecutionNode.getLatestBlockNumber),
  T.chainFirstIOK(() => () => {
    ExecutionNode.subscribeNewHeads(BlocksNewBlock.onNewBlock);
    Log.debug("listening and queuing new chain heads for analysis");
  }),
  T.bind("_fastSyncBlocks", ({ chainHeadOnStart }) =>
    pipe(
      Log.debugT(`fast-sync blocks up to ${chainHeadOnStart}`),
      T.chain(() => BlocksSync.syncBlocks(chainHeadOnStart)),
      T.chain(() => Log.debugT("fast-sync blocks done")),
    ),
  ),
  T.bind("_syncBurnRecords", () =>
    pipe(
      BurnRecordsSync.sync(),
      Performance.measureTaskPerf("sync burn records"),
      T.chainIOK(() => Log.debugIO("sync burn records done")),
    ),
  ),
  T.apS("_initEthStaked", EthStaked.init),
  T.apS("_initEthSupply", EthSupply.init),
  T.bind("_initLeaderboardLimitedTimeframes", () =>
    pipe(
      initLeaderboardLimitedTimeframes,
      Performance.measureTaskPerf("init leaderboard limited timeframes"),
    ),
  ),
  T.bind("_initLeaderboardAll", () =>
    pipe(
      Log.debugT("adding missing blocks to leaderboard all"),
      T.chain(() => T.of(LeaderboardsUnlimitedTimeframe.addMissingBlocks("all"))),
      T.chain(() =>
        Log.debugT("done adding missing blocks to leaderboard all"),
      ),
      Performance.measureTaskPerf("init leaderboard all"),
    ),
  ),
  T.bind("_initLeaderboardSinceMerge", () =>
    pipe(
      Log.debugT("adding missing blocks to leaderboard since_merge"),
      T.chain(() => T.of(LeaderboardsUnlimitedTimeframe.addMissingBlocks("since_merge"))),
      T.chain(() =>
        Log.debugT("done adding missing blocks to leaderboard since_merge"),
      ),
      Performance.measureTaskPerf("init leaderboard since_merge"),
    ),
  ),
  T.bind("_syncNextOnStart", ({ lastStoredBlockOnStart, chainHeadOnStart }) =>
    pipe(
      SyncOnStart.sync(lastStoredBlockOnStart.number + 1, chainHeadOnStart),
      Performance.measureTaskPerf("sync-next on start"),
    ),
  ),
  T.chain(() =>
    pipe(BlockLag.init, Performance.measureTaskPerf("init block lag")),
  ),
  // Start the queue after all the initial syncs are done. Although the we
  // started listening on a websocket and will continually put new blocks on
  // the queue, keeping it running forever, we currently don't have a way to
  // infinitely await the queue, so we simply complete this function, knowing
  // our program will keep running after our main function completes.
  T.map(() => {
    BlocksNewBlock.headsQueue.start();
    Log.info("started analyzing new blocks from queue");
  }),
);

// Gracefully handle init errors.
await pipe(
  TE.tryCatch(main, ErrAlt.unknownToError),
  TE.match(
    (e) => {
      Log.error("main task error", e);
      ExecutionNode.closeConnections();
      Db.closeConnection();
    },
    () => Log.info("init done"),
  ),
)();
