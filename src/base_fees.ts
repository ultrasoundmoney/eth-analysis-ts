import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import { BlockV1 } from "./blocks/blocks.js";
import { O } from "./fp.js";
import { sum } from "./numbers.js";
import type { TransactionReceiptV1, TransactionSegments } from "./transactions";
import * as Transactions from "./transactions.js";

export type FeeSegments = {
  /** fees burned for the creation of contracts. */
  creationsSum: number;
  /** fees burned for use of contracts. */
  contractSumsEth: Map<string, number>;
  /** fees burned for use of contracts in USD. */
  contractSumsUsd: Map<string, number> | undefined;
  /** fees burned for simple transfers. */
  transfersSum: number;
};

/**
 * Map of base fees grouped by contract address
 */
type ContractBaseFeeMap = Map<string, number>;

const sumPerContractEth = (
  block: BlockV1,
  txrs: TransactionReceiptV1[],
): ContractBaseFeeMap =>
  pipe(
    txrs,
    A.reduce(new Map<string, number>(), (sumMap, txr: TransactionReceiptV1) =>
      pipe(
        txr.to,
        O.match(
          () => sumMap,
          (to) =>
            pipe(sumMap.get(to) ?? 0, (currentSum) =>
              sumMap.set(to, currentSum + Transactions.calcBaseFee(block, txr)),
            ),
        ),
      ),
    ),
  );

const sumPerContractUsd = (
  block: BlockV1,
  txrs: TransactionReceiptV1[],
  ethPrice: number,
): ContractBaseFeeMap =>
  pipe(
    txrs,
    A.reduce(new Map<string, number>(), (sumMap, txr: TransactionReceiptV1) =>
      pipe(
        txr.to,
        O.match(
          () => sumMap,
          (to) =>
            pipe(sumMap.get(to) ?? 0, (currentSum) =>
              sumMap.set(
                to,
                currentSum +
                  (Transactions.calcBaseFee(block, txr) / 10 ** 18) * ethPrice,
              ),
            ),
        ),
      ),
    ),
  );

export const calcBlockBaseFeeSum = (block: BlockV1): bigint =>
  block.gasUsedBI * block.baseFeePerGasBI;

export const sumFeeSegments = (
  block: BlockV1,
  segments: TransactionSegments,
  ethPrice: number,
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
    A.reduce(0, (sum, txr) => sum + Transactions.calcBaseFee(block, txr)),
  );

  const contractSumsEth = sumPerContractEth(block, other);

  const contractSumsUsd = sumPerContractUsd(block, other, ethPrice);

  return {
    contractSumsEth,
    contractSumsUsd,
    creationsSum,
    transfersSum,
  };
};

export const getTip = (block: BlockV1, txr: TransactionReceiptV1) =>
  txr.gasUsed * txr.effectiveGasPrice - txr.gasUsed * block.baseFeePerGas;

export const getTipBI = (block: BlockV1, txr: TransactionReceiptV1) =>
  txr.gasUsedBI * txr.effectiveGasPriceBI -
  txr.gasUsedBI * block.baseFeePerGasBI;

export const calcBlockTips = (
  block: BlockV1,
  txrs: TransactionReceiptV1[],
): number =>
  pipe(
    txrs,
    A.map((txr) => getTip(block, txr)),
    sum,
  );
