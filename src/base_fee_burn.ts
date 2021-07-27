import { sql } from "./db.js";
import type { TxRWeb3London } from "./transactions";
import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import * as Log from "./log.js";

export type BaseFees = {
  // fees burned for simple transfers.
  transfers: number;
  // fees burned for use of contracts.
  contract_use_fees: Record<string, number>;
  // fees burned for the creation of contracts.
  contract_creation_fees: number;
};

export const getLatestAnalyzedBlockNumber = (): Promise<number | undefined> =>
  sql`
    SELECT max(number) AS number FROM base_fees_per_block
  `.then((result) => result[0]?.number || undefined);

export const storeBaseFeesForBlock = (
  hash: string,
  number: number,
  baseFees: BaseFees,
): Promise<void> =>
  sql`
    INSERT INTO base_fees_per_block
      (hash, number, base_fees)
    VALUES (${hash}, ${number}, ${sql.json(baseFees)})
`.then(() => undefined);

const hexToNumber = (hex: string) => Number.parseInt(hex, 16);
const weiToEth = (wei: number): number => wei / 10 ** 18;

const calculateBaseFee = (baseFee: string, txr: TxRWeb3London): number =>
  pipe(
    baseFee,
    hexToNumber,
    weiToEth,
    (baseFeePerGas) => baseFeePerGas * txr.gasUsed,
  );

export const calcTxrBaseFee = (
  baseFee: string,
  txrs: TxRWeb3London[],
): number =>
  pipe(
    txrs,
    A.reduce(0, (sum, txr) => sum + calculateBaseFee(baseFee, txr)),
  );

/**
 * Map of base fees grouped by contract address
 */
type ContractBaseFeeMap = Record<string, number>;

export const calcContractUseBaseFees = (
  baseFee: string,
  txrs: TxRWeb3London[],
): ContractBaseFeeMap =>
  pipe(
    txrs,
    A.reduce({} as ContractBaseFeeMap, (feeSumMap, txr) => {
      // Contract creation
      if (txr.to === null) {
        return feeSumMap;
      }

      const baseFeeSum = feeSumMap[txr.to] || 0;
      feeSumMap[txr.to] = baseFeeSum + calculateBaseFee(baseFee, txr);

      return feeSumMap;
    }),
  );

// Name is undefined because we don't always know the name for a contract. Image is undefined because we don't always have an image for a contract. Address is undefined because base fees paid for ETH transfers are shared between many addresses.
export type BaseFeeBurner = {
  name: string | undefined;
  address: string | undefined;
  image: string | undefined;
  fees: number;
};

// As block time changes these counts become inaccurate. It'd be better to store actual datetimes for blocks so precise time questions could be answered.
export type TimeFrame = "24h" | "7d" | "30d" | "all";
const timeFrameBlockCountMap: Record<TimeFrame, number> = {
  "24h": 6545,
  "7d": 45818,
  "30d": 196364,
  // NOTE: We use 100d as the current hard limit
  all: 654545,
};

export const getTopTenFeeBurners = async (
  timeFrame: TimeFrame,
): Promise<BaseFeeBurner[]> => {
  const blocksToSumCount = timeFrameBlockCountMap[timeFrame];
  const baseFeesPerBlock = await sql<{ baseFees: BaseFees }[]>`
      SELECT base_fees
      FROM base_fees_per_block
      LIMIT ${blocksToSumCount}
  `.then((rows) => {
    if (rows.length === 0) {
      Log.warn(
        "tried to determine top fee burners but found no analyzed blocks",
      );
      return [];
    }

    return rows.map((row) => row.baseFees);
  });

  const ethTransferBaseFees = baseFeesPerBlock.map(
    (baseFees) => baseFees.transfers,
  );
  const contractCreationBaseFees = baseFeesPerBlock.map(
    (baseFees) => baseFees.contract_creation_fees,
  );
  const contractBaseFeeMaps = baseFeesPerBlock.map(
    (baseFees) => baseFees.contract_use_fees,
  );

  const ethTransferBurnTotal = ethTransferBaseFees.reduce(
    (sum, fee) => sum + fee,
    0,
  );
  const contractCreationBurnTotal = contractCreationBaseFees.reduce(
    (sum, fee) => sum + fee,
    0,
  );

  const contractBurnerTotals: BaseFeeBurner[] = pipe(
    contractBaseFeeMaps,
    A.reduce({} as Record<string, number>, (agg, contractBaseFeeMap) => {
      Object.entries(contractBaseFeeMap).forEach(([address, fee]) => {
        const sum = agg[address] || 0;
        agg[address] = sum + fee;
      });
      return agg;
    }),
    Object.entries,
    A.map(([address, fees]) => ({
      image: undefined,
      name: undefined,
      address,
      fees,
    })),
  );

  return pipe(
    [
      {
        image: undefined,
        name: "ETH transfers",
        fees: ethTransferBurnTotal,
        address: undefined,
      },
      {
        image: undefined,
        name: "Contract deployments",
        fees: contractCreationBurnTotal,
        address: undefined,
      },
      ...contractBurnerTotals,
    ],
    A.sort<BaseFeeBurner>({
      compare: (first, second) =>
        first.fees === second.fees ? 0 : first.fees > second.fees ? -1 : 1,
      equals: (first, second) => first.fees === second.fees,
    }),
    A.takeLeft(10),
  );
};
