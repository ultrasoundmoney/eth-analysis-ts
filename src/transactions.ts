import PQueue from "p-queue";
import { setTimeout } from "timers/promises";
import * as Blocks from "./blocks/blocks.js";
import * as Duration from "./duration.js";
import * as ExecutionNode from "./execution_node.js";
import { A, flow, NEA, O, pipe, RA, T, TO } from "./fp.js";
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
  rawTrx: ExecutionNode.RawTxr,
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

export const fetchReceiptQueue = new PQueue({
  concurrency: 64,
});

const queueFetchReceipt =
  <A>(task: T.Task<A>): T.Task<A> =>
  () =>
    fetchReceiptQueue.add(task);

// This policy is kept quite strict. We'd like to retry the rare network issue, instead of crashing the whole service. However, we also expects rollbacks, which mean a transaction receipt no longer exists on-chain, and retrying will never succeed. Because it is hard to tell which is which, we simply retry with a low timeout.
const receiptRetryPolicy = capDelay(
  2000,
  RetryMonoid.concat(exponentialBackoff(100), limitRetries(2)),
);

export class TransactionReceiptNullError extends Error {}

export const transactionReceiptsFromBlock = (
  block: Blocks.BlockNodeV2,
): TE.TaskEither<
  TransactionReceiptNullError,
  readonly TransactionReceiptV1[]
> => {
  return pipe(
    block.transactions,
    TE.traverseArray((txHash) =>
      pipe(
        () => ExecutionNode.getTransactionReceipt(txHash),
        T.map(O.fromNullable),
        TE.fromTaskOption(
          () => new TransactionReceiptNullError(`txr for ${txHash} was null`),
        ),
        TE.map(transactionReceiptFromRaw),
        queueOnQueueT(fetchReceiptQueue),
      ),
    ),
    (te) => retrying(receiptRetryPolicy, () => te, E.isLeft),
  );
};

export const getTransactionReceiptsSafe = (block: Blocks.BlockNodeV2) =>
  pipe(
    block.transactions,
    TO.traverseArray((hash) =>
      pipe(
        () => ExecutionNode.getTransactionReceipt(hash),
        queueFetchReceipt,
        T.map(O.fromNullable),
      ),
    ),
    TO.map(RA.map(transactionReceiptFromRaw)),
    TO.map(RA.toArray),
  );

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
  block: Blocks.BlockNodeV2,
  txr: TransactionReceiptV1,
): number => block.baseFeePerGas * txr.gasUsed;

export const calcBaseFeeBI = (
  block: Blocks.BlockNodeV2,
  txr: TransactionReceiptV1,
) => BigInt(block.baseFeePerGas) * txr.gasUsedBI;

export const getNewContracts = flow(
  segmentTransactions,
  (segments) => segments.creations,
  A.map((txr) => txr.contractAddress),
  A.compact,
  NEA.fromArray,
);
