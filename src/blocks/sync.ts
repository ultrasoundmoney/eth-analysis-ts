import _ from "lodash";
import makeEta from "simple-eta";
import * as EthPrices from "../eth_prices.js";
import * as Log from "../log.js";
import * as PerformanceMetrics from "../performance_metrics.js";
import * as Transactions from "../transactions.js";
import * as Blocks from "./blocks.js";
import { rollbackToBefore } from "./new_head.js";

const syncBlock = async (blockNumber: number): Promise<void> => {
  const block = await Blocks.getBlockWithRetry(blockNumber);

  const isParentKnown = await Blocks.getBlockHashIsKnown(block.parentHash);

  // A rollback happened, before or during syncing. Roll back to the last known parent, sync up to the current block, and continue.
  if (!isParentKnown && !(blockNumber === Blocks.londonHardForkBlockNumber)) {
    Log.warn(
      "sync block parent is not in our DB, rolling back one block and trying again",
    );
    await rollbackToBefore(blockNumber - 1);
    await syncBlock(blockNumber - 1);
  }

  const [txrs, ethPrice] = await Promise.all([
    Transactions.getTxrsWithRetry(block),
    EthPrices.getPriceForOldBlock(block),
  ]);

  await Blocks.storeBlock(block, txrs, ethPrice.ethusd);
};

const rollbackToLastValidBlock = async () => {
  let lastStoredBlock = await Blocks.getLastStoredBlock();
  let block = await Blocks.getBlockWithRetry(lastStoredBlock.number);

  while (lastStoredBlock.hash !== block.hash) {
    Log.warn(
      `on-start last known block does not match chain, rolling back ${block.number}`,
    );
    await rollbackToBefore(lastStoredBlock.number - 1);
    lastStoredBlock = await Blocks.getLastStoredBlock();
    block = await Blocks.getBlockWithRetry(lastStoredBlock.number);
  }
};

export const syncBlocks = async (upToIncluding: number): Promise<void> => {
  // If a rollback happened while we were offline, rollback to the last valid block.
  await rollbackToLastValidBlock();

  const syncedBlockHeight = await Blocks.getSyncedBlockHeight();

  if (syncedBlockHeight === upToIncluding) {
    Log.debug("blocks table already in-sync with chain");
    return;
  }

  if (syncedBlockHeight > upToIncluding) {
    Log.debug(`chain head: ${upToIncluding}, synced to: ${syncedBlockHeight}`);
    throw new Error("chain head is behind blocks table?!");
  }

  const blocksToSync = _.range(syncedBlockHeight + 1, upToIncluding + 1);

  Log.debug(
    `blocks table sync ${blocksToSync.length} blocks, start: ${_.first(
      blocksToSync,
    )}, end: ${_.last(blocksToSync)}`,
  );

  const eta = makeEta({ max: blocksToSync.length });
  let blocksDone = 0;

  const logEta = _.throttle(() => {
    eta.report(blocksDone);
    Log.debug(`sync missing blocks, eta: ${eta.estimate()}s`);
  }, 8000);

  for (const blockNumber of blocksToSync) {
    await syncBlock(blockNumber);
    blocksDone = blocksDone + 1;
    logEta;
  }

  PerformanceMetrics.setShouldLogBlockFetchRate(false);
};
