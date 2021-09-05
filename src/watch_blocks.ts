import "@sentry/tracing";
import * as Blocks from "./blocks.js";
import * as DisplayProgress from "./display_progress.js";
import * as EthNode from "./eth_node.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import * as T from "fp-ts/lib/Task.js";
import Config from "./config.js";
import Sentry from "@sentry/node";
import { O } from "./fp.js";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { BlockLondon } from "./eth_node.js";
import { seqTPar } from "./sequence.js";

if (Config.env !== "dev") {
  Sentry.init({
    dsn: "https://f6393dc2e2984ec09299406e8f409647@o920717.ingest.sentry.io/5896630",
    tracesSampleRate: 0.1,
    environment: Config.env,
  });
}

PerformanceMetrics.setReportPerformance(true);

const syncBlocks = async (latestBlockNumberOnStart: number) => {
  const knownBlocksNumbers = await Blocks.getKnownBlocks()();
  const knownBlocks = new Set(knownBlocksNumbers);
  Log.debug(`${knownBlocks.size} known blocks`);

  EthNode.subscribeNewHeads((head) =>
    Blocks.storeBlockQueueSeq.add(
      Blocks.storeNewBlock(knownBlocks, head.number),
    ),
  );
  Log.info("listening for and adding new blocks");

  Log.debug("checking for missing blocks");
  const wantedBlockRange = Blocks.getBlockRange(
    Blocks.londonHardForkBlockNumber,
    latestBlockNumberOnStart,
  );

  const missingBlocks = wantedBlockRange.filter(
    (wantedBlockNumber) => !knownBlocks.has(wantedBlockNumber),
  );

  if (missingBlocks.length === 0) {
    return;
  } else {
    Log.info("blocks table out-of-sync");

    Log.info(`adding ${missingBlocks.length} missing blocks`);

    if (process.env.SHOW_PROGRESS !== undefined) {
      DisplayProgress.start(missingBlocks.length);
    }

    await Blocks.storeBlockQueuePar.addAll(
      missingBlocks.map((blockNumber) =>
        Blocks.storeNewBlock(knownBlocks, blockNumber),
      ),
    );
  }
};

const syncLeaderboardAll = async () => {
  Log.info("adding missing blocks to leaderboard all");
  return pipe(
    Blocks.getLatestStoredBlockNumber(),
    T.chain(
      O.match(
        () => {
          Log.warn(
            "no latest stored block, building leaderboard all block by block",
          );
          return T.of(undefined);
        },
        (latestStoredBlockNumber) =>
          LeaderboardsAll.addMissingBlocks(latestStoredBlockNumber),
      ),
    ),
    T.chainIOK(() => () => {
      Blocks.addLeaderboardAllQueue.start();
    }),
  );
};

const loadLeaderboardLimitedTimeframes = async (
  latestBlockNumberOnStart: number,
) => {
  return pipe(
    T.fromIO(() => {
      Log.info("loading leaderboards for limited timeframes");
    }),
    T.chain(() =>
      LeaderboardsLimitedTimeframe.addAllBlocksForAllTimeframes(
        latestBlockNumberOnStart,
      ),
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

    await seqTPar(
      () => syncBlocks(latestBlockNumberOnStart),
      () => loadLeaderboardLimitedTimeframes(latestBlockNumberOnStart),
      () => syncLeaderboardAll(),
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
