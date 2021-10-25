import "@sentry/tracing";
import * as Blocks from "./blocks.js";
import * as EthNode from "./eth_node.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as T from "fp-ts/lib/Task.js";
import { config } from "./config.js";
import Sentry from "@sentry/node";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { seqTParT } from "./fp.js";

if (config.env !== "dev") {
  Sentry.init({
    dsn: "https://f6393dc2e2984ec09299406e8f409647@o920717.ingest.sentry.io/5896630",
    tracesSampleRate: 0.1,
    environment: config.env,
  });
}

PerformanceMetrics.setShouldLogBlockFetchRate(true);

const syncBlocks = async (latestBlockNumberOnStart: number) => {
  EthNode.subscribeNewHeads((head) =>
    Blocks.storeBlockQueueSeq.add(Blocks.storeNewBlock(head.number)),
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
    await EthNode.connect();
    Log.debug("starting watch blocks");
    const latestBlockNumberOnStart = await EthNode.getLatestBlockNumber();
    Log.debug(`latest block on start: ${latestBlockNumberOnStart}`);

    await syncBlocks(latestBlockNumberOnStart);
    await seqTParT(
      loadLeaderboardLimitedTimeframes(latestBlockNumberOnStart),
      syncLeaderboardAll(latestBlockNumberOnStart),
    )();
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
