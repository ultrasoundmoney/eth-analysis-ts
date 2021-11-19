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

if (Config.getEnv() !== "dev") {
  Sentry.init({
    dsn: "https://f6393dc2e2984ec09299406e8f409647@o920717.ingest.sentry.io/5896630",
    tracesSampleRate: 0.1,
    environment: Config.getEnv(),
  });
}

PerformanceMetrics.setShouldLogBlockFetchRate(true);

const syncBlocks = async (latestBlockNumberOnStart: number) => {
  EthNode.subscribeNewHeads((head) =>
    Blocks.storeNewBlockQueue.add(Blocks.storeNewBlock(head.number)),
  );
  Log.info("listening for and adding new blocks");
  await Blocks.addMissingBlocks(latestBlockNumberOnStart);
  Log.info("done adding missing blocks");
};

const syncLeaderboardAll = (latestBlockNumberOnStart: number): T.Task<void> => {
  Log.info("adding missing blocks to leaderboard all");
  return pipe(
    LeaderboardsAll.addMissingBlocks(latestBlockNumberOnStart),
    T.chainIOK(() => () => {
      Log.info("done adding missing blocks to leaderboard all");
      Blocks.addLeaderboardAllQueue.start();
    }),
  );
};

const loadLeaderboardLimitedTimeframes = (
  latestBlockNumberOnStart: number,
): T.Task<void> => {
  return pipe(
    Log.info("loading leaderboards for limited timeframes"),
    () =>
      LeaderboardsLimitedTimeframe.addAllBlocksForAllTimeframes(
        latestBlockNumberOnStart,
      ),
    T.chainIOK(() => () => {
      Blocks.addLeaderboardLimitedTimeframeQueue.start();
      Log.info("done loading leaderboards for limited timeframes");
    }),
  );
};

const main = async () => {
  try {
    Config.ensureCriticalBlockAnalysisConfig();
    await EthNode.connect();
    Log.debug("starting watch blocks");
    const latestBlockNumberOnStart = await EthNode.getLatestBlockNumber();
    Log.debug(`latest block on start: ${latestBlockNumberOnStart}`);

    await syncBlocks(latestBlockNumberOnStart);
    await seqTParT(
      loadLeaderboardLimitedTimeframes(latestBlockNumberOnStart),
      syncLeaderboardAll(latestBlockNumberOnStart),
    )();

    EthPrices.continuouslyStorePrice();
    Coingecko.continuouslyStoreMarketCaps();
  } catch (error) {
    Log.error("error adding new blocks", { error });
    EthNode.closeConnection();
    sql.end();
    throw error;
  }
};

main();

process.on("unhandledRejection", (error) => {
  throw error;
});
