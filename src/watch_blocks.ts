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
import PQueue from "p-queue";
import Sentry from "@sentry/node";
import { O, TE } from "./fp.js";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { BlockLondon } from "./eth_node.js";
import { seqTPar, seqTSeq } from "./sequence.js";

if (Config.env !== "dev") {
  Sentry.init({
    dsn: "https://f6393dc2e2984ec09299406e8f409647@o920717.ingest.sentry.io/5896630",
    tracesSampleRate: 0.1,
    environment: Config.env,
  });
}

const storeBlockQueuePar = new PQueue({ concurrency: 8 });
const storeBlockQueueSeq = new PQueue({ concurrency: 1 });

PerformanceMetrics.setReportPerformance(true);

const syncBlocks = async (latestBlockOnStart: BlockLondon) => {
  const knownBlocksNumbers = await Blocks.getKnownBlocks()();
  const knownBlocks = new Set(knownBlocksNumbers);
  Log.debug(`${knownBlocks.size} known blocks`);

  EthNode.subscribeNewHeads((head) =>
    storeBlockQueueSeq.add(
      pipe(
        Blocks.storeNewBlock(knownBlocks, head.number, true),
        T.map(() => {
          knownBlocks.add(head.number);
        }),
      ),
    ),
  );
  Log.info("listening for and adding new blocks");

  Log.debug("checking for missing blocks");
  const wantedBlockRange = Blocks.getBlockRange(
    Blocks.londonHardForkBlockNumber,
    latestBlockOnStart.number,
  );

  const missingBlocks = wantedBlockRange.filter(
    (wantedBlockNumber) => !knownBlocks.has(wantedBlockNumber),
  );

  if (missingBlocks.length === 0) {
    Blocks.setSyncStatus("in-sync");
  } else {
    Log.info("blocks table out-of-sync");
    Blocks.setSyncStatus("out-of-sync");

    Log.info(`adding ${missingBlocks.length} missing blocks`);

    if (process.env.SHOW_PROGRESS !== undefined) {
      DisplayProgress.start(missingBlocks.length);
    }

    await storeBlockQueuePar.addAll(
      missingBlocks.map((blockNumber) =>
        Blocks.storeNewBlock(knownBlocks, blockNumber, false),
      ),
    );
    Blocks.setSyncStatus("in-sync");
  }
  Log.info("blocks table in-sync");
  PerformanceMetrics.setReportPerformance(false);
};

const syncLeaderboardAll = async (latestBlockOnStart: BlockLondon) => {
  Log.info("checking leaderboard all total in-sync");
  const newestIncludedBlockNumber =
    await LeaderboardsAll.getNewestIncludedBlockNumber()();
  if (
    O.isNone(newestIncludedBlockNumber) ||
    newestIncludedBlockNumber.value !== latestBlockOnStart.number
  ) {
    LeaderboardsAll.setSyncStatus("out-of-sync");
    await Blocks.addLeaderboardAllQueue.add(
      pipe(
        LeaderboardsAll.addMissingBlocks(),
        TE.mapLeft((e) => {
          if (e._tag === "no-blocks") {
            Log.warn("no latest stored block, won't update leaderboard!");
            return;
          }

          throw new Error(String(e));
        }),
      ),
    );
    LeaderboardsAll.setSyncStatus("in-sync");
  } else {
    LeaderboardsAll.setSyncStatus("in-sync");
  }
  Log.info("leaderboard all in-sync");
};

const loadLeaderboardLimitedTimeframes = async (
  latestBlockOnStart: BlockLondon,
) => {
  Log.info("loading leaderboards for limited timeframes");
  await LeaderboardsLimitedTimeframe.addAllBlocksForAllTimeframes(
    latestBlockOnStart.number,
  )();
  LeaderboardsLimitedTimeframe.setSyncStatus("in-sync");
  Log.info("done loading leaderboards for limited timeframes");
};

const main = async () => {
  try {
    await EthNode.connect();
    Log.debug("starting watch blocks");
    const latestBlockOnStart = await Blocks.getBlockWithRetry("latest");

    await seqTPar(
      seqTSeq(
        () => syncBlocks(latestBlockOnStart),
        // We can only load leaderboards when blocks are in-sync. We otherwise risk loading empty leaderboards as blocks are missing from the timeframes.
        () => loadLeaderboardLimitedTimeframes(latestBlockOnStart),
      ),
      () => syncLeaderboardAll(latestBlockOnStart),
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
