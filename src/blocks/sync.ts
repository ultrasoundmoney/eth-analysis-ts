import _ from "lodash";
import makeEta from "simple-eta";
import * as EthPrices from "../eth_prices.js";
import * as Log from "../log.js";
import * as PerformanceMetrics from "../performance_metrics.js";
import * as Transactions from "../transactions.js";
import * as Blocks from "./blocks.js";
import { addBlock } from "./new_head.js";

const syncBlock = async (blockNumber: number): Promise<void> => {
  Log.debug(`sync block: ${blockNumber}`);
  const block = await Blocks.getBlockWithRetry(blockNumber);
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
  const lastStoredBlock = await Blocks.getLastStoredBlock();

  // Check no rollback happened while we were offline
  const block = await Blocks.getBlockWithRetry(lastStoredBlock.number);
  if (block.hash !== lastStoredBlock.hash) {
    throw new Error(
      "last known block has been rolled back while we were offline",
    );
  }

  const syncedBlockHeight = await Blocks.getSyncedBlockHeight();

  if (syncedBlockHeight === upToIncluding) {
    Log.debug("blocks table already in-sync with chain");
    return;
  }

  if (syncedBlockHeight > upToIncluding) {
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
