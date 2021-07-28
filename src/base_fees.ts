import { sql } from "./db.js";
import type { TxRWeb3London } from "./transactions";
import A from "fp-ts/lib/Array.js";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import O from "fp-ts/lib/Option.js";
import R from "fp-ts/lib/Record.js";
import { flow, pipe } from "fp-ts/lib/function.js";
import * as Log from "./log.js";
import { hexToNumber, sum, weiToEth } from "./numbers.js";
import { getUnixTime, startOfDay } from "date-fns";
import type { BlockWeb3London } from "./blocks.js";

export type BlockBaseFees = {
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

export const storeBaseFeesForBlock = async (
  block: BlockWeb3London,
  baseFees: BlockBaseFees,
): Promise<void> => {
  const timestampNumber =
    typeof block.timestamp === "string"
      ? hexToNumber(block.timestamp)
      : block.timestamp;

  await sql`
    INSERT INTO base_fees_per_block
      (
        hash,
        number,
        base_fees,
        mined_at
      )
    VALUES
      (
        ${block.hash},
        ${block.number},
        ${sql.json(baseFees)},
        to_timestamp(${timestampNumber})
      )
  `;

  return undefined;
};

export const calcTxrBaseFee = (
  baseFeePerGas: string,
  txr: TxRWeb3London,
): number =>
  pipe(
    baseFeePerGas,
    hexToNumber,
    weiToEth,
    (baseFeePerGasNum) => baseFeePerGasNum * txr.gasUsed,
  );

/**
 * Map of base fees grouped by contract address
 */
type ContractBaseFeeMap = Record<string, number>;

export const calcBaseFeePerContract = (
  baseFeePerGas: string,
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
      feeSumMap[txr.to] = baseFeeSum + calcTxrBaseFee(baseFeePerGas, txr);

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
  const baseFeesPerBlock = await sql<{ baseFees: BlockBaseFees }[]>`
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

  const ethTransferBaseFees = pipe(
    baseFeesPerBlock,
    A.map((baseFees) => baseFees.transfers),
    sum,
  );
  const contractCreationBaseFees = pipe(
    baseFeesPerBlock,
    A.map((baseFees) => baseFees.contract_creation_fees),
    sum,
  );

  const contractBurnerTotals = pipe(
    baseFeesPerBlock,
    A.map((baseFees) => baseFees.contract_use_fees),
    // We merge Record<address, baseFees>[] here.
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
        fees: ethTransferBaseFees,
        address: undefined,
      },
      {
        image: undefined,
        name: "Contract deployments",
        fees: contractCreationBaseFees,
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

const calcBlockBaseFees = (baseFees: BlockBaseFees): number =>
  baseFees.transfers +
  sum(Object.values(baseFees.contract_use_fees)) +
  baseFees.contract_creation_fees;

export const getTotalFeesBurned = async (): Promise<number> => {
  const baseFeesPerBlock = await sql<{ baseFees: BlockBaseFees }[]>`
      SELECT base_fees
      FROM base_fees_per_block
  `.then((rows) => {
    if (rows.length === 0) {
      Log.warn("tried to get top fee burners before any blocks were analyzed");
    }

    return rows.map((row) => row.baseFees);
  });

  return pipe(baseFeesPerBlock, A.map(calcBlockBaseFees), sum);
};

export type FeesBurnedPerDay = Record<string, number>;

export const getFeesBurnedPerDay = async (): Promise<FeesBurnedPerDay> => {
  const rows = await sql<{ baseFees: BlockBaseFees; minedAt: Date }[]>`
      SELECT base_fees, mined_at
      FROM base_fees_per_block
  `.then((rows) => {
    if (rows.length === 0) {
      Log.warn(
        "tried to determine base fees per day, but found no analyzed blocks",
      );
    }

    return rows;
  });

  const mBlocks = NEA.fromArray(rows);

  if (O.isNone(mBlocks)) {
    return {};
  }

  const blocks = mBlocks.value;

  return pipe(
    blocks,
    NEA.groupBy((block) =>
      pipe(block.minedAt, startOfDay, getUnixTime, String),
    ),
    R.map(
      flow(
        NEA.map((block) => block.baseFees),
        NEA.map(calcBlockBaseFees),
        sum,
      ),
    ),
  );
};

export const notifyNewBaseFee = (block: BlockWeb3London): Promise<void> =>
  sql
    .notify(
      "base-fee-updates",
      JSON.stringify({
        number: block.number,
        baseFeePerGas: hexToNumber(block.baseFeePerGas),
      }),
    )
    .then(() => undefined);
