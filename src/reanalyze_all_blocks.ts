import PQueue from "p-queue";
import * as Blocks from "./blocks.js";
import { sql } from "./db.js";
import * as DisplayProgress from "./display_progress.js";
import * as EthNode from "./eth_node.js";
import * as Log from "./log.js";
import * as Transactions from "./transactions.js";

const storeBlockQueuePar = new PQueue({ concurrency: 8 });

const reanalyzeAllBlocks = async () => {
  const latestBlockNumber = await EthNode.getLatestBlockNumber();
  Log.debug(`latest block is ${latestBlockNumber}`);

  const blocksToAnalyze = Blocks.getBlockRange(
    Blocks.londonHardForkBlockNumber,
    latestBlockNumber,
  );

  if (process.env.SHOW_PROGRESS !== undefined) {
    DisplayProgress.start(blocksToAnalyze.length);
  }

  Log.debug(`${blocksToAnalyze.length} blocks to analyze`);

  await storeBlockQueuePar.addAll(
    blocksToAnalyze.map((blockNumber) => async () => {
      const block = await Blocks.getBlockWithRetry(blockNumber);
      const txrs = await Transactions.getTxrsWithRetry(block);
      await Blocks.updateBlock(block, txrs)();
      if (process.env.SHOW_PROGRESS !== undefined) {
        DisplayProgress.onBlockAnalyzed();
      }
    }),
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
