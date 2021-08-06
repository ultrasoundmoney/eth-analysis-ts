import * as eth from "./web3.js";
// eslint-disable-next-line node/no-unpublished-import
import type { TransactionReceipt as TxRWeb3 } from "web3-core";
import PQueue from "p-queue";
import * as Log from "./log.js";
import { delay } from "./delay.js";

/**
 * A post London hardfork transaction receipt with an effective gas price.
 */
export type TxRWeb3London = TxRWeb3 & {
  to: string | null;
  effectiveGasPrice: string;
};

// Depending on 'when' you call the next two functions the receipt looks different. We leave it up to the caller to call this function at the right time.
const getTxr = async (txHash: string): Promise<TxRWeb3> => {
  const txr = await eth.getTransactionReceipt(txHash);
  // NOTE: Seen in production. Unclear why this would happen. Should we retry? Are some transactions not executed resulting in `null` transaction receipts? Needs investigation.
  if (txr === null) {
    Log.warn(`txr for ${txHash} is null, waiting 2s and trying again`);
    await delay(2000);
    const rawTxr2 = await eth.getTransactionReceipt(txHash);
    if (rawTxr2 === null) {
      throw new Error("Transaction Receipt came back as null");
    } else {
      return rawTxr2;
    }
  }

  return txr;
};

const getTxr1559 = (txHash: string): Promise<TxRWeb3London> =>
  getTxr(txHash) as Promise<TxRWeb3London>;

export const getTxrs = (txHashes: string[]): Promise<TxRWeb3[]> =>
  txrsPQ.addAll(txHashes.map((txHash) => () => getTxr(txHash)));

const txrsPQ = new PQueue({
  concurrency: 64,
});

export const getTxrs1559 = (txHashes: string[]): Promise<TxRWeb3London[]> =>
  txrsPQ.addAll(txHashes.map((txHash) => () => getTxr1559(txHash)));

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
