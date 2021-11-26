import Sentry from "@sentry/node";
import "@sentry/tracing";
import { pipe } from "fp-ts/lib/function.js";
import * as T from "fp-ts/lib/Task.js";
import * as Blocks from "./blocks.js";
import * as Coingecko from "./coingecko.js";
import * as Config from "./config.js";
import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";
import * as EthPrices from "./eth_prices.js";
import { seqTParT } from "./fp.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as PerformanceMetrics from "./performance_metrics.js";

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

const syncLeaderboardAll = (): T.Task<void> => {
  Log.info("adding missing blocks to leaderboard all");
  return pipe(
    LeaderboardsAll.addMissingBlocks(),
    T.chainIOK(() => () => {
      Log.info("done adding missing blocks to leaderboard all");
    }),
  );
};

const loadLeaderboardLimitedTimeframes = (): T.Task<void> => {
  return pipe(
    Log.info("loading leaderboards for limited timeframes"),
    () => LeaderboardsLimitedTimeframe.addAllBlocksForAllTimeframes(),
    T.chainIOK(
      () => () => Log.info("done loading leaderboards for limited timeframes"),
    ),
  );
};

try {
  Config.ensureCriticalBlockAnalysisConfig();
  await EthNode.connect();
  Log.debug("started processing new blocks");

  EthNode.subscribeNewHeads((head) =>
    Blocks.storeNewBlockQueue.add(Blocks.storeNewBlock(head.number)),
  );
  Log.info("listening for and queueing new blocks to add");
  await Blocks.addMissingBlocks()();
  Log.info("done adding missing blocks");

  await seqTParT(loadLeaderboardLimitedTimeframes(), syncLeaderboardAll())();

  Blocks.storeNewBlockQueue.start();
} catch (error) {
  Log.error("error adding new blocks", { error });
  EthNode.closeConnection();
  sql.end();
  throw error;
}
