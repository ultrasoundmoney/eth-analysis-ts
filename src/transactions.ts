import { eth } from "./web3.js";
import type { TransactionReceipt as TxRWeb3 } from "web3-eth/types/index";
import { pipe } from "fp-ts/lib/function.js";
import T from "fp-ts/lib/Task.js";
import PQueue from "p-queue";

/**
 * A post London hardfork transaction receipt with an effective gas price.
 */
export type TxRWeb3London = TxRWeb3 & {
  to: string | null;
  effectiveGasPrice: string;
};

// Depending on 'when' you call the next two functions the receipt looks different. We leave it up to the caller to call this function at the right time.
const getTxr =
  (txHash: string): T.Task<TxRWeb3> =>
  () =>
    eth.getTransactionReceipt(txHash);

const getTxr1559 = (txHash: string): T.Task<TxRWeb3London> =>
  getTxr(txHash) as T.Task<TxRWeb3London>;

export const getTxrs = (txHashes: string[]): T.Task<readonly TxRWeb3[]> =>
  pipe(txHashes, T.traverseSeqArray(getTxr));

const txrsPQ = new PQueue({
  concurrency: 4,
});

export const getTxrs1559 = (
  txHashes: string[],
): T.Task<readonly TxRWeb3London[]> =>
  pipe(
    txHashes,
    T.traverseArray((txHash) => () => txrsPQ.add(getTxr1559(txHash))),
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
