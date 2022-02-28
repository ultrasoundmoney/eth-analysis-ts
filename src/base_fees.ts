import { pipe } from "fp-ts/lib/function.js";
import { BlockV1 } from "./blocks/blocks.js";
import { A, O } from "./fp.js";
import { sum } from "./numbers.js";
import type { TransactionReceiptV1, TransactionSegments } from "./transactions";
import * as Transactions from "./transactions.js";

export type FeeSegments = {
  /** fees burned for the creation of contracts. */
  creationsSum: number;
  /** fees burned for use of contracts. */
  contractSumsEth: Map<string, number>;
  /** fees burned for use of contracts, bigint */
  contractSumsEthBI: Map<string, bigint>;
  /** fees burned for use of contracts in USD. */
  contractSumsUsd: Map<string, number> | undefined;
  /** fees burned for simple transfers. */
  transfersSum: number;
};

/**
 * Map of base fees grouped by contract address
 */
type ContractBaseFeeMap = Map<string, number>;
type ContractBaseFeeMapBI = Map<string, bigint>;

const mergeReceiptEth = (
  block: BlockV1,
  sumMap: ContractBaseFeeMap,
  transactionReceipt: TransactionReceiptV1,
) =>
  pipe(
    transactionReceipt.to,
    O.match(
      () => sumMap,
      (to) =>
        pipe(
          sumMap.get(to) ?? 0,
          (currentSum) =>
            currentSum + Transactions.calcBaseFee(block, transactionReceipt),
          (nextSum) => sumMap.set(to, nextSum),
        ),
    ),
  );

const sumPerContractEth = (
  block: BlockV1,
  transactionReceipts: TransactionReceiptV1[],
): ContractBaseFeeMap =>
  pipe(
    transactionReceipts,
    A.reduce(
      new Map<string, number>(),
      (sumMap, transactionReceipt: TransactionReceiptV1) =>
        mergeReceiptEth(block, sumMap, transactionReceipt),
    ),
  );

const mergeReceiptEthBI = (
  block: BlockV1,
  sumMap: ContractBaseFeeMapBI,
  transactionReceipt: TransactionReceiptV1,
) =>
  pipe(
    transactionReceipt.to,
    O.match(
      () => sumMap,
      (to) =>
        pipe(
          sumMap.get(to) ?? 0n,
          (currentSum) =>
            currentSum + Transactions.calcBaseFeeBI(block, transactionReceipt),
          (nextSum) => sumMap.set(to, nextSum),
        ),
    ),
  );

const sumPerContractEthBI = (
  block: BlockV1,
  transactionReceipts: TransactionReceiptV1[],
): ContractBaseFeeMapBI =>
  pipe(
    transactionReceipts,
    A.reduce(new Map<string, bigint>(), (sumMap, transactionReceipt) =>
      mergeReceiptEthBI(block, sumMap, transactionReceipt),
    ),
  );

const mergeReceiptUsd = (
  block: BlockV1,
  sumMap: ContractBaseFeeMap,
  transactionReceipt: TransactionReceiptV1,
  ethPrice: number,
) =>
  pipe(
    transactionReceipt.to,
    O.match(
      () => sumMap,
      (to) =>
        pipe(
          sumMap.get(to) ?? 0,
          (currentSum) =>
            currentSum +
            (Transactions.calcBaseFee(block, transactionReceipt) / 10 ** 18) *
              ethPrice,
          (nextSum) => sumMap.set(to, nextSum),
        ),
    ),
  );

const sumPerContractUsd = (
  block: BlockV1,
  transactionReceipts: TransactionReceiptV1[],
  ethPrice: number,
): ContractBaseFeeMap =>
  pipe(
    transactionReceipts,
    A.reduce(
      new Map<string, number>(),
      (sumMap, transactionReceipt: TransactionReceiptV1) =>
        mergeReceiptUsd(block, sumMap, transactionReceipt, ethPrice),
    ),
  );

export const calcBlockBaseFeeSum = (block: BlockV1): bigint =>
  block.gasUsedBI * block.baseFeePerGasBI;

export const sumFeeSegments = (
  block: BlockV1,
  segments: TransactionSegments,
  ethPrice?: number,
): FeeSegments => {
  const { creations: creations, transfers: transfers, other: other } = segments;

  const transfersSum = pipe(
    transfers,
    A.reduce(
      0,
      (sum, transactionReceipt) =>
        sum + Transactions.calcBaseFee(block, transactionReceipt),
    ),
  );

  const creationsSum = pipe(
    creations,
    A.reduce(
      0,
      (sum, transactionReceipt) =>
        sum + Transactions.calcBaseFee(block, transactionReceipt),
    ),
  );

  const contractSumsEth = sumPerContractEth(block, other);

  const contractSumsEthBI = sumPerContractEthBI(block, other);

  // Temporarily allow no eth price for precise contract base fee migration.
  const contractSumsUsd =
    ethPrice === undefined
      ? new Map()
      : sumPerContractUsd(block, other, ethPrice);

  return {
    contractSumsEth,
    contractSumsEthBI,
    contractSumsUsd,
    creationsSum,
    transfersSum,
  };
};

export const getTip = (
  block: BlockV1,
  transactionReceipt: TransactionReceiptV1,
) =>
  transactionReceipt.gasUsed * transactionReceipt.effectiveGasPrice -
  transactionReceipt.gasUsed * block.baseFeePerGas;

export const getTipBI = (
  block: BlockV1,
  transactionReceipt: TransactionReceiptV1,
) =>
  transactionReceipt.gasUsedBI * transactionReceipt.effectiveGasPriceBI -
  transactionReceipt.gasUsedBI * block.baseFeePerGasBI;

export const calcBlockTips = (
  block: BlockV1,
  transactionReceipts: TransactionReceiptV1[],
): number =>
  pipe(
    transactionReceipts,
    A.map((transactionReceipt) => getTip(block, transactionReceipt)),
    sum,
  );
