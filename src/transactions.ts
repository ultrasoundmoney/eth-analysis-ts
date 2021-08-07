import * as eth from "./web3.js";
// eslint-disable-next-line node/no-unpublished-import
import type { TransactionReceipt as TxRWeb3 } from "web3-core";
import PQueue from "p-queue";
import * as Log from "./log.js";
import { delay } from "./delay.js";
import * as Sentry from "@sentry/node";
import { BlockLondon } from "./web3.js";

/**
 * A post London hardfork transaction receipt with an effective gas price.
 */
export type TxRWeb3London = TxRWeb3 & {
  to: string | null;
  effectiveGasPrice: string;
};

export type TxrsFetchResult =
  | { type: "txrs"; txrs: TxRWeb3London[] }
  | { type: "missing-txrs"; missingHashes: string[] };

const txrsPQ = new PQueue({
  concurrency: 64,
});

export const getTxrsWithRetry = async (
  block: BlockLondon,
): Promise<TxRWeb3London[]> => {
  let firstErrorReported = false;

  // Retry continuously
  let tryBlock = block;
  let txrs: TxRWeb3London[] = [];
  let missingHashes: string[] = [];

  while (txrs.length === 0 || missingHashes.length !== 0) {
    await txrsPQ.addAll(
      block.transactions.map(
        (txHash) => () =>
          eth.getTransactionReceipt(txHash).then((txr) => {
            if (txr === undefined) {
              missingHashes.push(txHash);
            } else {
              txrs.push(txr);
            }
          }),
      ),
    );

    // Had missing receipts again. Report, wait, then retry.
    if (missingHashes.length !== 0) {
      if (!firstErrorReported) {
        firstErrorReported = true;
        Sentry.captureMessage("block had null txrs", {
          extra: {
            number: tryBlock.number,
            hash: tryBlock.hash,
            missingHashes,
          },
        });
      }

      const delayMilis = 3000;
      Log.warn(
        `block cointained null txrs, waiting ${
          delayMilis / 1000
        }s and trying again`,
        {
          number: tryBlock.number,
          hash: tryBlock.hash,
          missingHashes: missingHashes,
        },
      );
      await delay(delayMilis);

      // Maybe the block got forked and that's why the receipts are null?  Refetch the block.
      tryBlock = await eth.getBlock(block.number);

      // Empty accumulated results
      missingHashes = [];
      txrs = [];
    }
  }

  return txrs;
};

export const getTxrsUnsafe = async (
  block: BlockLondon,
): Promise<TxRWeb3London[]> => {
  const missingHashes: string[] = [];
  const txrs: TxRWeb3London[] = [];

  await txrsPQ.addAll(
    block.transactions.map(
      (txHash) => () =>
        eth.getTransactionReceipt(txHash).then((txr) => {
          if (txr === undefined) {
            missingHashes.push(txHash);
          } else {
            txrs.push(txr);
          }
        }),
    ),
  );

  if (missingHashes.length !== 0) {
    throw new Error("block had null txrs");
  }

  return txrs;
};

export type TxrSegments = {
  contractCreationTxrs: TxRWeb3London[];
  ethTransferTxrs: TxRWeb3London[];
  contractUseTxrs: TxRWeb3London[];
};

export const segmentTxrs = (txrs: readonly TxRWeb3London[]): TxrSegments => {
  const contractUseTxrs: TxRWeb3London[] = [];
  const contractCreationTxrs: TxRWeb3London[] = [];
  const ethTransferTxrs: TxRWeb3London[] = [];

  txrs.forEach((txr) => {
    if (txr.to === null) {
      contractCreationTxrs.push(txr);
    } else if (txr.gasUsed === 21000) {
      ethTransferTxrs.push(txr);
    } else {
      contractUseTxrs.push(txr);
    }
  });

  return { contractCreationTxrs, contractUseTxrs, ethTransferTxrs };
};
