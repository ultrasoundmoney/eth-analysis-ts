import { eth } from "./web3.js";
import type { TransactionReceipt as TxRWeb3 } from "web3-eth/types/index";
import { flow, pipe } from "fp-ts/lib/function";
import T from "fp-ts/lib/Task";
import ProgressBar from "progress";
import Config from "./config.js";

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
): T.Task<readonly TxRWeb3London[]> => {
  // On dev show progress fetching transaction receipts.
  let bar: ProgressBar | undefined = undefined;
  if (Config.env === "dev") {
    bar = new ProgressBar(">> [:bar] :rate/s :percent :etas", {
      total: txHashes.length,
    });
    const timer = setInterval(() => {
      if (bar?.complete) {
        clearInterval(timer);
      }
    }, 100);
  }

  return pipe(
    txHashes,
    T.traverseSeqArray(
      flow(
        getTxr1559,
        T.chainFirst(() => {
          bar?.tick();
          return T.of(undefined);
        }),
      ),
    ),
  );
};
