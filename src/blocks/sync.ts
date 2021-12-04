import PQueue from "p-queue";
import makeEta from "simple-eta";
import * as EthNode from "../eth_node.js";
import * as EthPrices from "../eth_prices.js";
import { A, B, pipe, T, TAlt } from "../fp.js";
import * as Log from "../log.js";
import * as PerformanceMetrics from "../performance_metrics.js";
import * as Transactions from "../transactions.js";
import * as Blocks from "./blocks.js";
import { getBlockHashIsKnown, getBlockWithRetry } from "./blocks.js";
import { storeNewBlock } from "./store_new_block.js";

export const syncBlockQueue = new PQueue({ concurrency: 1 });

const syncBlock = (blockNumber: number): T.Task<void> => {
  return pipe(
    () => getBlockWithRetry(blockNumber),
    T.chainFirst((block) =>
      pipe(
        getBlockHashIsKnown(block.parentHash),
        T.chain(
          B.match(
            // We're missing the parent hash, update the previous block.
            () =>
              pipe(
                () =>
                  Log.warn(
                    "addMissingBlock, parent hash not found, storing parent again",
                  ),
                () => storeNewBlock(blockNumber - 1),
              ),
            () => T.of(undefined),
          ),
        ),
      ),
    ),
    T.chain((block) =>
      TAlt.seqTParT(
        T.of(block),
        () => Transactions.getTxrsWithRetry(block),
        EthPrices.getPriceForOldBlock(block),
      ),
    ),
    T.chain(([block, txrs, ethPrice]) =>
      Blocks.storeBlock(block, txrs, ethPrice?.ethusd),
    ),
  );
};

export const syncBlocks = (
  upToNumber: number | undefined = undefined,
): T.Task<void> =>
  pipe(
    TAlt.seqTParT(
      () => EthNode.getLatestBlockNumber(),
      Blocks.getKnownBlocks(),
    ),
    T.chainFirstIOK(([latestBlockNumber]) => () => {
      Log.debug(`syncing blocks table with chain, head: ${latestBlockNumber}`);
    }),
    T.map(([latestBlockNumber, knownBlocks]) =>
      pipe(
        Blocks.getBlockRange(
          Blocks.londonHardForkBlockNumber,
          upToNumber || latestBlockNumber,
        ),
        A.filter((number) => !knownBlocks.has(number)),
      ),
    ),
    T.chain((missingBlocks) => {
      if (missingBlocks.length === 0) {
        Log.debug("blocks table already in-sync with chain");
        return T.of(undefined);
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

      return () => syncBlockQueue.addAll(missingBlocks.map(syncBlock));
    }),
    T.chainFirstIOK(() => () => {
      PerformanceMetrics.setShouldLogBlockFetchRate(false);
    }),
    T.map(() => undefined),
  );
