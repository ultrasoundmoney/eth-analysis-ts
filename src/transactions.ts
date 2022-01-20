import PQueue from "p-queue";
import { setTimeout } from "timers/promises";
import * as Blocks from "./blocks/blocks.js";
import * as Duration from "./duration.js";
import * as EthNode from "./eth_node.js";
import { O } from "./fp.js";
import * as Hexadecimal from "./hexadecimal.js";
import * as Log from "./log.js";
import * as PerformanceMetrics from "./performance_metrics.js";

/**
 * A post London hardfork transaction receipt with an effective gas price.
 */
export type TransactionReceiptV1 = {
  blockNumber: number;
  contractAddress: O.Option<string>;
  effectiveGasPrice: number;
  effectiveGasPriceBI: bigint;
  gasUsed: number;
  gasUsedBI: bigint;
  to: O.Option<string>;
  transactionHash: string;
};

export const transactionReceiptFromRaw = (
  rawTrx: EthNode.RawTxr,
): TransactionReceiptV1 => ({
  blockNumber: Hexadecimal.numberFromHex(rawTrx.blockNumber),
  contractAddress: O.fromNullable(rawTrx.contractAddress),
  effectiveGasPrice: Hexadecimal.numberFromHex(rawTrx.effectiveGasPrice),
  effectiveGasPriceBI: BigInt(rawTrx.effectiveGasPrice),
  gasUsed: Hexadecimal.numberFromHex(rawTrx.gasUsed),
  gasUsedBI: BigInt(rawTrx.gasUsed),
  to: O.fromNullable(rawTrx.to),
  transactionHash: rawTrx.transactionHash,
});

export const txrsPQ = new PQueue({
  concurrency: 64,
});

export const getTxrsWithRetry = async (
  block: Blocks.BlockV1,
): Promise<TransactionReceiptV1[]> => {
  let tries = 0;

  // Retry continuously
  let tryBlock = block;
  let txrs: TransactionReceiptV1[] = [];
  let missingHashes: string[] = [];

  // eslint-disable-next-line no-constant-condition
  while (true) {
    tries += tries + 1;

    await txrsPQ.addAll(
      tryBlock.transactions.map(
        (txHash) => () =>
          EthNode.getTransactionReceipt(txHash).then((txr) => {
            if (txr === null) {
              missingHashes.push(txHash);
            } else {
              PerformanceMetrics.onTxrReceived();
              txrs.push(transactionReceiptFromRaw(txr));
            }
          }),
      ),
    );

    if (missingHashes.length === 0) {
      break;
    }

    const delayMilis = Duration.millisFromSeconds(3);

    if (tries === 10) {
      Log.alert(
        "failed to fetch transaction receipts",
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
      `block ${tryBlock.number} contained null txrs, hash: ${tryBlock.hash}, waiting ${delaySeconds}s and trying again`,
    );
    await setTimeout(delayMilis);

    // Maybe the block got forked and that's why the receipts are null?  Refetch the block.
    tryBlock = await Blocks.getBlockWithRetry(block.number);

    // Empty accumulated results
    missingHashes = [];
    txrs = [];
  }

  return txrs;
};

export type TxrSegments = {
  contractCreationTxrs: TransactionReceiptV1[];
  ethTransferTxrs: TransactionReceiptV1[];
  contractUseTxrs: TransactionReceiptV1[];
};

export const segmentTxrs = (
  txrs: readonly TransactionReceiptV1[],
): TxrSegments => {
  const contractUseTxrs: TransactionReceiptV1[] = [];
  const contractCreationTxrs: TransactionReceiptV1[] = [];
  const ethTransferTxrs: TransactionReceiptV1[] = [];

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
