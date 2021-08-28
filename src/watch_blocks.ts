import Sentry from "@sentry/node";
import "@sentry/tracing";
import * as Blocks from "./blocks.js";
import * as DisplayProgress from "./display_progress.js";
import * as EthNode from "./eth_node.js";
import * as Log from "./log.js";
import * as T from "fp-ts/lib/Task.js";
import Config from "./config.js";
import PQueue from "p-queue";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";

if (Config.env !== "dev") {
  Sentry.init({
    dsn: "https://f6393dc2e2984ec09299406e8f409647@o920717.ingest.sentry.io/5896630",
    tracesSampleRate: 0.1,
    environment: Config.env,
  });
}

const storeBlockQueuePar = new PQueue({ concurrency: 8 });
const storeBlockQueueSeq = new PQueue({ concurrency: 1 });

const watchBlocks = async () => {
  Log.debug("starting watch blocks");
  const knownBlocksNumbers = await Blocks.getKnownBlocks()();
  const knownBlocks = new Set(knownBlocksNumbers);
  Log.debug(`${knownBlocks.size} known blocks`);

  const latestBlockOnStart = await Blocks.getBlockWithRetry("latest");
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
    Log.info("blocks table in-sync");
    return;
  }

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
  Log.info("blocks table in-sync");
};

const main = async () => {
  try {
    await EthNode.connect();
    await watchBlocks();
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
