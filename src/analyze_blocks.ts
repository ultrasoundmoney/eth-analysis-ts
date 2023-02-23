import * as Blocks from "./blocks/blocks.js";
import Koa from "koa";
import * as BlockLag from "./block_lag.js";
import * as BlocksNewBlock from "./blocks/new_head.js";
import * as BlocksSync from "./blocks/sync.js";
import * as BurnRecordsSync from "./burn-records/sync.js";
import * as Config from "./config.js";
import { runMigrations, sql } from "./db.js";
import * as ExecutionNode from "./execution_node.js";
import * as Leaderboards from "./leaderboards.js";
import * as Log from "./log.js";
import * as Performance from "./performance.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as EthStaked from "./scarcity/eth_staked.js";
import * as EthSupply from "./scarcity/eth_supply.js";
import * as SyncOnStart from "./sync_on_start.js";
import * as Db from "./db.js";

PerformanceMetrics.setShouldLogBlockFetchRate(true);

const initLeaderboardLimitedTimeframes = async (): Promise<void> => {
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
  Config.ensureCriticalBlockAnalysisConfig();
  await runMigrations();

  await startHealthCheckServer();

  const lastStoredBlockOnStart = await Blocks.getLastStoredBlock()();
  const chainHeadOnStart = await ExecutionNode.getLatestBlockNumber();
  Log.debug(`fast-sync blocks up to ${chainHeadOnStart}`);

  ExecutionNode.subscribeNewHeads(BlocksNewBlock.onNewBlock);
  Log.debug("listening and queuing new chain heads for analysis");

  await BlocksSync.syncBlocks(chainHeadOnStart);
  Log.info("fast-sync blocks done");

  await Performance.measurePromisePerf(
    "sync burn records",
    BurnRecordsSync.sync()(),
  );
  await EthStaked.init();
  await EthSupply.init();
  await Performance.measurePromisePerf(
    "init leaderboard limited timeframes",
    initLeaderboardLimitedTimeframes(),
  );
  await Performance.measurePromisePerf(
    "sync-next on start",
    SyncOnStart.sync(lastStoredBlockOnStart.number + 1, chainHeadOnStart)(),
  );
  await Performance.measurePromisePerf("init block lag", BlockLag.init());

  BlocksNewBlock.headsQueue.start();
  Log.info("started analyzing new blocks from queue");
} catch (error) {
  ExecutionNode.closeConnections();
  sql.end();
  throw error;
}
