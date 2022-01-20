import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import * as ROA from "fp-ts/lib/ReadonlyArray.js";
import { BlockDb } from "./blocks/blocks.js";
import { BlockLondon } from "./eth_node.js";
import { hexToNumber } from "./hexadecimal.js";
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
  block: BlockLondon,
  txr: TransactionReceiptV1,
): number => hexToNumber(block.baseFeePerGas) * txr.gasUsed;

export const calcTxrBaseFeeBI = (
  block: BlockLondon,
  txr: TransactionReceiptV1,
) => BigInt(block.baseFeePerGas) * txr.gasUsedBI;

/**
 * Map of base fees grouped by contract address
 */
type ContractBaseFeeMap = Map<string, number>;

const calcBaseFeePerContract = (
  block: BlockLondon,
  txrs: TransactionReceiptV1[],
): ContractBaseFeeMap =>
  pipe(
    txrs,
    A.reduce(new Map(), (sumMap, txr: TransactionReceiptV1) =>
      sumMap.set(
        txr.to,
        (sumMap.get(txr.to) || 0) + calcTxrBaseFee(block, txr),
      ),
    ),
  );

const calcBaseFeePerContractUsd = (
  block: BlockLondon,
  txrs: TransactionReceiptV1[],
  ethPrice: number,
): ContractBaseFeeMap =>
  pipe(
    txrs,
    A.reduce(new Map(), (sumMap, txr: TransactionReceiptV1) =>
      sumMap.set(
        txr.to,
        (sumMap.get(txr.to) || 0) +
          (calcTxrBaseFee(block, txr) / 10 ** 18) * ethPrice,
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

export const calcBlockBaseFeeSum = (block: BlockLondon): bigint =>
  block.gasUsedBI * block.baseFeePerGasBI;

export const calcBlockBaseFeeSumDb = (block: BlockDb): bigint =>
  block.gasUsed * block.baseFeePerGas;

export const calcBlockFeeBreakdown = (
  block: BlockLondon,
  transactionReceiptSegments: Transactions.TxrSegments,
  ethPrice?: number,
): FeeBreakdown => {
  const { contractCreationTxrs, ethTransferTxrs, contractUseTxrs } =
    transactionReceiptSegments;

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
  block: BlockLondon,
  txrs: readonly TransactionReceiptV1[],
): number => {
  return pipe(
    txrs,
    ROA.map(
      (txr) =>
        txr.gasUsed * hexToNumber(txr.effectiveGasPrice) -
        txr.gasUsed * hexToNumber(block.baseFeePerGas),
    ),
    sum,
  );
};
