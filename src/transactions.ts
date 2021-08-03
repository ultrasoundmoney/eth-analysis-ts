import * as eth from "./web3.js";
// eslint-disable-next-line node/no-unpublished-import
import type { TransactionReceipt as TxRWeb3 } from "web3-core";
import { pipe } from "fp-ts/lib/function.js";
import T from "fp-ts/lib/Task.js";
import ROA from "fp-ts/lib/ReadonlyArray.js";
import O from "fp-ts/lib/Option.js";
import PQueue from "p-queue";

/**
 * A post London hardfork transaction receipt with an effective gas price.
 */
export type TxRWeb3London = TxRWeb3 & {
  to: string | null;
  effectiveGasPrice: string;
};

// Depending on 'when' you call the next two functions the receipt looks different. We leave it up to the caller to call this function at the right time.
const getTxr = (txHash: string): T.Task<O.Option<TxRWeb3>> =>
  pipe(() => eth.getTransactionReceipt(txHash), T.map(O.fromNullable));

const getTxr1559 = (txHash: string): T.Task<O.Option<TxRWeb3London>> =>
  getTxr(txHash) as T.Task<O.Option<TxRWeb3London>>;

export const getTxrs = (txHashes: string[]): T.Task<readonly TxRWeb3[]> =>
  // NOTE: we skip null transactions. See web3 module for details.
  pipe(txHashes, T.traverseSeqArray(getTxr), T.map(ROA.compact));

const txrsPQ = new PQueue({
  concurrency: 64,
});

export const getTxrs1559 = (
  txHashes: string[],
): T.Task<readonly TxRWeb3London[]> =>
  pipe(
    txHashes,
    T.traverseArray((txHash) => () => txrsPQ.add(getTxr1559(txHash))),
    // NOTE: we skip null transactions. See web3 module for details.
    T.map(ROA.compact),
  );

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
