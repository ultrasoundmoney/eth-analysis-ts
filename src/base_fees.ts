import { sql } from "./db.js";
import type { TxRWeb3London } from "./transactions";
import A from "fp-ts/lib/Array.js";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import R from "fp-ts/lib/Record.js";
import { pipe } from "fp-ts/lib/function.js";
import * as Log from "./log.js";
import { sum } from "./numbers.js";
import { BlockLondon } from "./eth_node.js";
import neatCsv from "neat-csv";
import fs from "fs/promises";
import * as Transactions from "./transactions.js";
import * as ROA from "fp-ts/lib/ReadonlyArray.js";
import { hexToNumber } from "./hexadecimal.js";
import { fromUnixTime, isAfter, subDays, subHours } from "date-fns";

export type FeeBreakdown = {
  /** fees burned for simple transfers. */
  transfers: number;
  /** fees burned for use of contracts. */
  contract_use_fees: Partial<Record<string, number>>;
  /** fees burned for the creation of contracts. */
  contract_creation_fees: number;
};

export const calcTxrBaseFee = (
  block: BlockLondon,
  txr: TxRWeb3London,
): number => hexToNumber(block.baseFeePerGas) * txr.gasUsed;

/**
 * Map of base fees grouped by contract address
 */
type ContractBaseFeeMap = Record<string, number>;

const calcBaseFeePerContract = (
  block: BlockLondon,
  txrs: TxRWeb3London[],
): ContractBaseFeeMap =>
  pipe(
    txrs,
    A.reduce({} as ContractBaseFeeMap, (feeSumMap, txr: TxRWeb3London) => {
      // Contract creation
      if (txr.to === null) {
        return feeSumMap;
      }

      const baseFeeSum = feeSumMap[txr.to] || 0;
      feeSumMap[txr.to] = baseFeeSum + calcTxrBaseFee(block, txr);

      return feeSumMap;
    }),
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

let contractNameMap: Partial<Record<string, string>> | undefined = undefined;
export const getContractNameMap = async () => {
  if (contractNameMap !== undefined) {
    return contractNameMap;
  }

  const knownContracts = await neatCsv<{ dapp: string; address: string }>(
    await fs.readFile("./master_list.csv"),
  );

  contractNameMap = pipe(
    knownContracts,
    NEA.groupBy((knownContract) => knownContract.address),
    R.map((knownContractsForAddress) => knownContractsForAddress[0].dapp),
  );

  return contractNameMap;
};

export const calcBlockBaseFeeSum = (block: BlockLondon): number =>
  block.gasUsed * hexToNumber(block.baseFeePerGas);

export const calcBlockFeeBreakdown = (
  block: BlockLondon,
  txrs: readonly TxRWeb3London[],
): FeeBreakdown => {
  const { contractCreationTxrs, ethTransferTxrs, contractUseTxrs } =
    Transactions.segmentTxrs(txrs);

  const ethTransferFees = pipe(
    ethTransferTxrs,
    A.map((txr) => calcTxrBaseFee(block, txr)),
    sum,
  );

  const contractCreationFees = pipe(
    contractCreationTxrs,
    A.map((txr) => calcTxrBaseFee(block, txr)),
    sum,
  );

  const feePerContract = calcBaseFeePerContract(block, contractUseTxrs);

  return {
    transfers: ethTransferFees,
    contract_use_fees: feePerContract,
    contract_creation_fees: contractCreationFees,
  };
};

export const calcBlockTips = (
  block: BlockLondon,
  txrs: readonly TxRWeb3London[],
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
