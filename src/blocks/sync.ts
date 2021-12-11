import _ from "lodash";
import makeEta from "simple-eta";
import * as EthPrices from "../eth_prices.js";
import * as Log from "../log.js";
import * as PerformanceMetrics from "../performance_metrics.js";
import * as Transactions from "../transactions.js";
import * as Blocks from "./blocks.js";
import { getBlockWithRetry } from "./blocks.js";
import { addBlock } from "./new_head.js";

const syncBlock = async (blockNumber: number): Promise<void> => {
  Log.debug(`sync block: ${blockNumber}`);
  const block = await getBlockWithRetry(blockNumber);
  const isParentKnown = await Blocks.getBlockHashIsKnown(block.parentHash);

  if (!isParentKnown) {
    // TODO: should never happen anymore, remove this if no alert shows up.
    // We're missing the parent hash, update the previous block.
    Log.alert("sync block, parent hash not found, storing parent again");
    const previousBlock = await Blocks.getBlockWithRetry(blockNumber - 1);
    await addBlock(previousBlock);
  }

  const [txrs, ethPrice] = await Promise.all([
    Transactions.getTxrsWithRetry(block),
    EthPrices.getPriceForOldBlock(block),
  ]);

  await Blocks.storeBlock(block, txrs, ethPrice.ethusd);
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

  Log.debug(
    `blocks table sync ${missingBlocks.length} blocks, start: ${
      missingBlocks[0]
    }, end: ${_.last(missingBlocks)}`,
  );

  const eta = makeEta({ max: missingBlocks.length });
  let blocksDone = 0;

  const id = setInterval(() => {
    eta.report(blocksDone);
    if (blocksDone === missingBlocks.length) {
      clearInterval(id);
      return;
    }
    Log.debug(`sync missing blocks, eta: ${eta.estimate()}s`);
  }, 8000);

  for (const missingBlock of missingBlocks) {
    await syncBlock(missingBlock);
    blocksDone = blocksDone + 1;
  }

  PerformanceMetrics.setShouldLogBlockFetchRate(false);
};
