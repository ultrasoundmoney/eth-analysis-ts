import _ from "lodash";
import makeEta from "simple-eta";
import * as EthPrices from "../eth-prices/index.js";
import { O, pipe, TEAlt, TOAlt } from "../fp.js";
import * as Log from "../log.js";
import * as PerformanceMetrics from "../performance_metrics.js";
import * as Transactions from "../transactions.js";
import * as Blocks from "./blocks.js";
import { rollbackToIncluding } from "./new_head.js";

export const syncBlock = async (blockNumber: number): Promise<void> => {
  Log.info(`Syncing block: ${blockNumber}`);
  const block = await Blocks.getBlockWithRetry(blockNumber);

  const isParentKnown = await Blocks.getBlockHashIsKnown(block.parentHash);

  // A rollback happened, before or during syncing. Roll back to the last known parent, sync up to the current block, and continue.
  if (!isParentKnown && !(blockNumber === Blocks.londonHardForkBlockNumber)) {
    Log.warn(
      "sync block parent is not in our DB, rolling back one block and trying again",
    );
    const parentBlockNumber = blockNumber - 1;
    const oBlock = await Blocks.getBlock(parentBlockNumber)();
    if (O.isSome(oBlock)) {
      await rollbackToIncluding(oBlock.value)();
    }
    await syncBlock(blockNumber - 1);
  }

  const [txrs, ethPrice] = await Promise.all([
    Transactions.getTxrsWithRetry(block),
    pipe(EthPrices.getEthPrice(block.timestamp), TEAlt.getOrThrow)(),
  ]);

  await Blocks.storeBlock(block, txrs, ethPrice.ethusd);
};

const rollbackToLastValidBlock = async () => {
  let lastStoredBlock = await Blocks.getLastStoredBlock()();
  let block = await Blocks.getBlockSafe(lastStoredBlock.number)();

  while (O.isNone(block) || lastStoredBlock.hash !== block.value.hash) {
    Log.warn(
      `on-start last known block does not match chain, rolling back ${lastStoredBlock.number}`,
    );
    const parentBlockNumber = lastStoredBlock.number - 1;
    const oBlock = await Blocks.getBlock(parentBlockNumber)();
    if (O.isSome(oBlock)) {
      await rollbackToIncluding(oBlock.value)();
    }
    lastStoredBlock = await Blocks.getLastStoredBlock()();
    block = await Blocks.getBlockSafe(lastStoredBlock.number)();
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

  // Happens sometimes when multiple instances of analyze-blocks are running.
  if (syncedBlockHeight > upToIncluding) {
    throw new Error(
      "failed to sync, blocks table is further ahead than requested sync point",
    );
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
    logEta();
  }

  PerformanceMetrics.setShouldLogBlockFetchRate(false);
};
