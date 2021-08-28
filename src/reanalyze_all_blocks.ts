import * as Blocks from "./blocks.js";
import * as DisplayProgress from "./display_progress.js";
import * as EthNode from "./eth_node.js";
import * as Log from "./log.js";
import { sql } from "./db.js";
import PQueue from "p-queue";

const storeBlockQueuePar = new PQueue({ concurrency: 8 });

const reanalyzeAllBlocks = async () => {
  const latestBlock = await Blocks.getBlockWithRetry("latest");
  Log.debug(`latest block is ${latestBlock.number}`);

  const blocksToAnalyze = Blocks.getBlockRange(
    Blocks.londonHardForkBlockNumber,
    latestBlock.number,
  );

  if (process.env.SHOW_PROGRESS !== undefined) {
    DisplayProgress.start(blocksToAnalyze.length);
  }

  Log.debug(`${blocksToAnalyze.length} blocks to analyze`);

  // We assume all blocks up to the latest block are blocks we have in the DB, or will have in the DB by the time we get around to reanalyzing the latest blocks.
  const knownBlocks = new Set(blocksToAnalyze);

  await storeBlockQueuePar.addAll(
    blocksToAnalyze.map((blockNumber) =>
      Blocks.storeNewBlock(knownBlocks, blockNumber, false),
    ),
  );
};

const main = async () => {
  try {
    Log.info("reanalyzing all blocks");
    await EthNode.connect();
    await reanalyzeAllBlocks();
    Log.info("done reanalyzing all blocks");
  } catch (error) {
    Log.error("error reanalyzing all blocks", { error });
    throw error;
  } finally {
    EthNode.closeConnection();
    sql.end();
  }
};

main();
