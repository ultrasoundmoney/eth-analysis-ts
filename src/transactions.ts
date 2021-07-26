import { eth } from "./web3";
import type { TransactionReceipt as TxRWeb3 } from "web3-eth/types/index";
import { pipe } from "fp-ts/lib/function";
import T from "fp-ts/lib/Task";

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

export const getTxrs1559 = (
  txHashes: string[],
): T.Task<readonly TxRWeb3London[]> =>
  pipe(txHashes, T.traverseSeqArray(getTxr1559));
