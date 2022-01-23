import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import * as ROA from "fp-ts/lib/ReadonlyArray.js";
import { BlockDb, BlockV1 } from "./blocks/blocks.js";
import { O } from "./fp.js";
import { sum } from "./numbers.js";
import type { TransactionReceiptV1 } from "./transactions";
import * as Transactions from "./transactions.js";

export type FeeBreakdown = {
  /** fees burned for simple transfers. */
  transfers: number;
  /** fees burned for use of contracts. */
  contract_use_fees: Map<string, number>;
  /** fees burned for use of contracts in USD. */
  contract_use_fees_usd: Map<string, number> | undefined;
  /** fees burned for the creation of contracts. */
  contract_creation_fees: number;
};

export const calcTxrBaseFee = (
  block: BlockV1,
  txr: TransactionReceiptV1,
): number => block.baseFeePerGas * txr.gasUsed;

export const calcTxrBaseFeeBI = (block: BlockV1, txr: TransactionReceiptV1) =>
  BigInt(block.baseFeePerGas) * txr.gasUsedBI;

/**
 * Map of base fees grouped by contract address
 */
type ContractBaseFeeMap = Map<string, number>;

const calcBaseFeePerContract = (
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
          (to) => {
            const currentSum = sumMap.get(to) ?? 0;
            return sumMap.set(to, currentSum + calcTxrBaseFee(block, txr));
          },
        ),
      ),
    ),
  );

const calcBaseFeePerContractUsd = (
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
          (to) => {
            const currentSum = sumMap.get(to) ?? 0;
            return sumMap.set(
              to,
              currentSum + (calcBaseFee(block, txr) / 10 ** 18) * ethPrice,
            );
          },
        ),
      ),
    ),
  );

export const sumFeeMaps = (
  maps: Partial<Record<string, number>>[],
): Record<string, number> =>
  (maps as Record<string, number>[]).reduce((sumMap, map) => {
    Object.entries(map as Record<string, number>).forEach(([key, num]) => {
      const sum = sumMap[key] || 0;
      sumMap[key] = sum + num;
    });
    return sumMap;
  }, {} as Record<string, number>);

export const calcBlockBaseFeeSum = (block: BlockV1): bigint =>
  block.gasUsedBI * block.baseFeePerGasBI;

export const calcBlockBaseFeeSumDb = (block: BlockDb): bigint =>
  block.gasUsed * block.baseFeePerGas;

export const calcBlockFeeBreakdown = (
  block: BlockV1,
  transactionReceiptSegments: Transactions.TxrSegments,
  ethPrice?: number,
): FeeBreakdown => {
  const {
    creation: contractCreationTxrs,
    transfer: ethTransferTxrs,
    other: contractUseTxrs,
  } = transactionReceiptSegments;

  const ethTransferFees = pipe(
    ethTransferTxrs,
    A.reduce(0, (sum, txr) => sum + calcTxrBaseFee(block, txr)),
  );

  const contractCreationFees = pipe(
    contractCreationTxrs,
    A.reduce(0, (sum, txr) => sum + calcTxrBaseFee(block, txr)),
  );

  const feePerContract = calcBaseFeePerContract(block, contractUseTxrs);
  const feePerContractUsd =
    ethPrice === undefined
      ? undefined
      : calcBaseFeePerContractUsd(block, contractUseTxrs, ethPrice);

  return {
    transfers: ethTransferFees,
    contract_use_fees: feePerContract,
    contract_use_fees_usd: feePerContractUsd,
    contract_creation_fees: contractCreationFees,
  };
};

export const calcBlockTips = (
  block: BlockV1,
  txrs: readonly TransactionReceiptV1[],
): number =>
  pipe(
    txrs,
    ROA.map(
      (txr) =>
        txr.gasUsed * txr.effectiveGasPrice - txr.gasUsed * block.baseFeePerGas,
    ),
    sum,
  );
