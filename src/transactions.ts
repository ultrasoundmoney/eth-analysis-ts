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

export const getTxrsWithRetry = async (
  block: Blocks.BlockNodeV2,
): Promise<TransactionReceiptV1[]> => {
  let tries = 0;
  let delayMilis = Duration.millisFromSeconds(1);

  // Retry continuously
  const tryBlock = block;
  const txrs: TransactionReceiptV1[] = [];
  let missingHashes: string[] = tryBlock.transactions;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    tries += tries + 1;

    const hashesToFetch = missingHashes;
    missingHashes = [];
    await fetchReceiptQueue.addAll(
      hashesToFetch.map(
        (txHash) => () =>
          ExecutionNode.getTransactionReceipt(txHash)
            .then((txr) => {
              if (txr === null) {
                missingHashes.push(txHash);
              } else {
                PerformanceMetrics.onTxrReceived();
                txrs.push(transactionReceiptFromRaw(txr));
              }
            })
            .catch(() => {
              missingHashes.push(txHash);
            }),
      ),
    );

    if (txrs.length === tryBlock.transactions.length) {
      Log.info(
        `Returning from getTxrsWithRetry. Fetched transactions: ${txrs.length} - missing transactions: ${missingHashes.length}`,
      );
      return txrs;
    } else {
      Log.warn(
        `${missingHashes.length} missing hashes after iteration ${tries}`,
      );
    }

    if (tries % 5 === 0) {
      delayMilis = delayMilis * 2;
      Log.debug(`Sleeping for ${delayMilis}ms`);
    }

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
  }
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
