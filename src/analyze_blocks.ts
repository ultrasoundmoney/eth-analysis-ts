import Koa from "koa";
import { pipe, T } from "./fp.js";
import * as Blocks from "./blocks/blocks.js";
import * as BlocksNewBlock from "./blocks/new_head.js";
import * as BlocksSync from "./blocks/sync.js";
import * as BlockLag from "./block_lag.js";
import * as BurnRecordsSync from "./burn-records/sync.js";
import * as Config from "./config.js";
import * as Db from "./db.js";
import * as ExecutionNode from "./execution_node.js";
import * as Leaderboards from "./leaderboards.js";
import * as Log from "./log.js";
import * as Performance from "./performance.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as EthStaked from "./scarcity/eth_staked.js";
import * as EthSupply from "./scarcity/eth_supply.js";
import * as SyncOnStart from "./sync_on_start.js";

PerformanceMetrics.setShouldLogBlockFetchRate(true);

const initLeaderboard = async (): Promise<void> => {
  Log.info("loading leaderboards for limited timeframes");
  await Leaderboards.addAllBlocksForAllTimeframes()();
  Log.info("done loading leaderboards for limited timeframes");
};

const startHealthCheckServer = async () => {
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

  await new Promise((resolve) => {
    app.listen(port, () => {
      resolve(undefined);
    });
  });

  Log.info(`listening on ${port}`);
};

try {
  await pipe(
    T.Do,
    T.apS(
      "_ensureCriticalConfig",
      T.fromIO(Config.ensureCriticalBlockAnalysisConfig),
    ),
    T.apS("_runMigrations", Db.runMigrations),
    T.apS("_startHealthCheckServer", startHealthCheckServer),
    T.apS("lastStoredBlockOnStart", () => Blocks.getLastStoredBlock()()),
    T.apS("chainHeadOnStart", ExecutionNode.getLatestBlockNumber),
    T.chainFirstIOK(({ chainHeadOnStart }) =>
      Log.debugIO(`fast-sync blocks up to ${chainHeadOnStart}`),
    ),
    T.chainFirstIOK(() => () => {
      ExecutionNode.subscribeNewHeads(BlocksNewBlock.onNewBlock);
      Log.debug("listening and queuing new chain heads for analysis");
    }),
    T.bind("_syncBlocks", ({ chainHeadOnStart }) =>
      pipe(
        () => BlocksSync.syncBlocks(chainHeadOnStart),
        T.chainIOK(() => Log.debugIO("fast-sync blocks done")),
      ),
    ),
    T.bind("_syncBurnRecords", () =>
      pipe(
        BurnRecordsSync.sync(),
        Performance.measureTaskPerf("sync burn records"),
        T.chainIOK(() => Log.debugIO("sync burn records done")),
      ),
    ),
    T.bind("_initEthStaked", () => EthStaked.init),
    T.bind("_initEthSupply", () => EthSupply.init),
    T.bind("_initLeaderboard", () =>
      pipe(
        initLeaderboard,
        Performance.measureTaskPerf("init leaderboard limited timeframes"),
      ),
    ),
    T.bind("_syncNextOnStart", ({ lastStoredBlockOnStart, chainHeadOnStart }) =>
      pipe(
        () =>
          SyncOnStart.sync(
            lastStoredBlockOnStart.number + 1,
            chainHeadOnStart,
          )(),
        Performance.measureTaskPerf("sync-next on start"),
      ),
    ),
    T.bind("_initBlockLag", () =>
      pipe(BlockLag.init, Performance.measureTaskPerf("init block lag")),
    ),
  )();
} catch (error) {
  ExecutionNode.closeConnections();
  Db.closeConnection();
  throw error;
}
