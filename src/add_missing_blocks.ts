import { sql } from "./db.js";
import * as Log from "./log.js";
import * as EthNode from "./eth_node.js";
import * as Blocks from "./blocks.js";
import * as DisplayProgress from "./display_progress.js";
import PQueue from "p-queue";

const storeBlockQueuePar = new PQueue({ concurrency: 8 });

const addMissingBlocks = async () => {
  Log.debug("starting watch blocks");
  const knownBlocksNumbers = await Blocks.getKnownBlocks()();
  const knownBlocks = new Set(knownBlocksNumbers);
  Log.debug(`${knownBlocks.size} known blocks`);

  const latestBlockOnStart = await Blocks.getBlockWithRetry("latest");

  Log.debug("checking for missing blocks");
  const wantedBlockRange = Blocks.getBlockRange(
    Blocks.londonHardForkBlockNumber,
    latestBlockOnStart.number,
  );

  const missingBlocks = wantedBlockRange.filter(
    (wantedBlockNumber) => !knownBlocks.has(wantedBlockNumber),
  );

  if (missingBlocks.length === 0) {
    Log.info("no missing blocks");
    return;
  }

  Log.info(`${missingBlocks.length} missing blocks, storing`);

  if (process.env.SHOW_PROGRESS !== undefined) {
    DisplayProgress.start(missingBlocks.length);
  }

  await storeBlockQueuePar.addAll(
    missingBlocks.map((blockNumber) =>
      Blocks.storeNewBlock(knownBlocks, blockNumber, false),
    ),
  );
  Log.info("done adding missing blocks");
};

const main = async () => {
  try {
    await EthNode.connect();
    await addMissingBlocks();
  } catch (error) {
    Log.error("error adding missing blocks", { error });
    throw error;
  } finally {
    EthNode.closeConnection();
    await sql.end();
  }
};

main();

process.on("unhandledRejection", (error) => {
  throw error;
});
