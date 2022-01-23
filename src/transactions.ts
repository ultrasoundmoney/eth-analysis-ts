import PQueue from "p-queue";
import { setTimeout } from "timers/promises";
import * as Blocks from "./blocks/blocks.js";
import * as Duration from "./duration.js";
import * as EthNode from "./eth_node.js";
import { A, flow, NEA, O } from "./fp.js";
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

export type TransactionSegments = {
  creations: TransactionReceiptV1[];
  transfers: TransactionReceiptV1[];
  other: TransactionReceiptV1[];
};

const getIsEthTransfer = (txr: TransactionReceiptV1) =>
  txr.gasUsedBI === 21000n;

const getIsContractCreation = (txr: TransactionReceiptV1) => O.isNone(txr.to);

export const segmentTransactions = (
  transactionReceipts: TransactionReceiptV1[],
): TransactionSegments => ({
  transfers: transactionReceipts.filter(getIsEthTransfer),
  creations: transactionReceipts.filter(getIsContractCreation),
  other: transactionReceipts.filter(
    (transactionReceipt) =>
      !getIsContractCreation(transactionReceipt) &&
      !getIsEthTransfer(transactionReceipt),
  ),
});

export const calcBaseFee = (
  block: Blocks.BlockV1,
  txr: TransactionReceiptV1,
): number => block.baseFeePerGas * txr.gasUsed;

export const calcBaseFeeBI = (
  block: Blocks.BlockV1,
  txr: TransactionReceiptV1,
) => BigInt(block.baseFeePerGas) * txr.gasUsedBI;

export const getNewContracts = flow(
  segmentTransactions,

  (segments) => segments.creations,
  A.map((txr) => txr.contractAddress),
  A.compact,
  NEA.fromArray,
);
