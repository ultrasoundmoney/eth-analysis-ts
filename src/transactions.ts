import * as Sentry from "@sentry/node";
import PQueue from "p-queue";
import type { TransactionReceipt as TxRWeb3 } from "web3-core";
import * as Blocks from "./blocks.js";
import { delay } from "./delay.js";
import * as Duration from "./duration.js";
import * as EthNode from "./eth_node.js";
import { BlockLondon } from "./eth_node.js";
import * as Log from "./log.js";
import * as PerformanceMetrics from "./performance_metrics.js";

/**
 * A post London hardfork transaction receipt with an effective gas price.
 */
export type TxRWeb3London = TxRWeb3 & {
  to: string | null;
  effectiveGasPrice: string;
};

export const txrsPQ = new PQueue({
  concurrency: 64,
});

export const getTxrsWithRetry = async (
  block: BlockLondon,
): Promise<TxRWeb3London[]> => {
  let tries = 0;

  // Retry continuously
  let tryBlock = block;
  let txrs: TxRWeb3London[] = [];
  let missingHashes: string[] = [];

  // eslint-disable-next-line no-constant-condition
  while (true) {
    tries += tries + 1;

    await txrsPQ.addAll(
      tryBlock.transactions.map(
        (txHash) => () =>
          EthNode.getTransactionReceipt(txHash).then((txr) => {
            if (txr === undefined) {
              missingHashes.push(txHash);
            } else {
              PerformanceMetrics.onTxrReceived();
              txrs.push(txr);
            }
          }),
      ),
    );

    if (missingHashes.length === 0) {
      break;
    }

    const delayMilis = Duration.millisFromSeconds(3);

    if (tries === 10) {
      Sentry.captureException(
        new Error(
          `stuck fetching transactions, for more than ${
            (tries * delayMilis) / 1000
          }s`,
        ),
      );
    }

    if (tries > 20) {
      throw new Error(
        "failed to fetch transactions for block, some stayed null",
      );
    }

    const delaySeconds = delayMilis / 1000;
    Log.warn(
      `block ${tryBlock.number} cointained null txrs, hash: ${tryBlock.hash}, waiting ${delaySeconds}s and trying again`,
    );
    await delay(delayMilis);

    // Maybe the block got forked and that's why the receipts are null?  Refetch the block.
    tryBlock = await Blocks.getBlockWithRetry(block.number);

    // Empty accumulated results
    missingHashes = [];
    txrs = [];
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
