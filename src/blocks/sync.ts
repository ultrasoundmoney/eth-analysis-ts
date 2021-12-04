import PQueue from "p-queue";
import makeEta from "simple-eta";
import * as EthPrices from "../eth_prices.js";
import * as Log from "../log.js";
import * as PerformanceMetrics from "../performance_metrics.js";
import * as Transactions from "../transactions.js";
import { analyzeNewBlock } from "./analyze_new_block.js";
import * as Blocks from "./blocks.js";
import { getBlockWithRetry } from "./blocks.js";

export const syncBlockQueue = new PQueue({ concurrency: 1 });

const syncBlock = async (blockNumber: number): Promise<void> => {
  const block = await getBlockWithRetry(blockNumber);
  const isParentKnown = await Blocks.getBlockHashIsKnown(block.parentHash);

  if (!isParentKnown) {
    // We're missing the parent hash, update the previous block.
    Log.warn("storeNewBlock, parent hash not found, storing parent again");
    await analyzeNewBlock(blockNumber - 1);
  }

  const [txrs, ethPrice] = await Promise.all([
    Transactions.getTxrsWithRetry(block),
    EthPrices.getPriceForOldBlock(block),
  ]);

  await Blocks.storeBlock(block, txrs, ethPrice.ethusd)();
};

export const syncBlocks = async (upToIncluding: number): Promise<void> => {
  const knownBlocks = await Blocks.getKnownBlocks()();
  Log.debug(`syncing blocks table up to: ${upToIncluding}`);

  const missingBlocks = Blocks.getBlockRange(
    Blocks.londonHardForkBlockNumber,
    upToIncluding,
  ).filter((num) => !knownBlocks.has(num));

  if (missingBlocks.length === 0) {
    Log.debug("blocks table already in-sync with chain");
    return undefined;
  }

  Log.debug(`blocks table sync ${missingBlocks.length} blocks`);

  const eta = makeEta({ max: missingBlocks.length });

  const id = setInterval(() => {
    eta.report(missingBlocks.length - syncBlockQueue.size);
    if (syncBlockQueue.size === 0) {
      clearInterval(id);
      return;
    }
    Log.debug(`sync missing blocks, eta: ${eta.estimate()}s`);
  }, 8000);

  await syncBlockQueue.addAll(
    missingBlocks.map((block) => () => syncBlock(block)),
  );

  PerformanceMetrics.setShouldLogBlockFetchRate(false);
};
