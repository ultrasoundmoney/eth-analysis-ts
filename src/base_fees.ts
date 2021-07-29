import { sql } from "./db.js";
import type { TxRWeb3London } from "./transactions";
import A from "fp-ts/lib/Array.js";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import O from "fp-ts/lib/Option.js";
import R from "fp-ts/lib/Record.js";
import { flow, pipe } from "fp-ts/lib/function.js";
import * as Log from "./log.js";
import { hexToNumber, sum } from "./numbers.js";
import { getUnixTime, startOfDay } from "date-fns";
import type { BlockLondon } from "./web3.js";
import neatCsv from "neat-csv";
import fs from "fs/promises";

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

const getBlockTimestamp = (block: BlockLondon): number => {
  // TODO: remove this if no errors are reported.
  if (typeof block.timestamp !== "number") {
    Log.error(
      `> block ${block.number} had unexpected timestamp: ${block.timestamp}`,
    );
  }

  return block.timestamp;
};

export const storeBaseFeesForBlock = async (
  block: BlockLondon,
  baseFees: BlockBaseFees,
): Promise<void> =>
  sql`
  INSERT INTO base_fees_per_block
    (hash, number, base_fees, mined_at)
  VALUES
    (
      ${block.hash},
      ${block.number},
      ${sql.json(baseFees)},
      to_timestamp(${getBlockTimestamp(block)})
    )
  `.then(() => undefined);

const toBaseFeeUnsafeInsert = ({
  block,
  baseFees,
}: {
  block: BlockLondon;
  baseFees: BlockBaseFees;
}) => `
  (
    '${block.hash}',
    '${String(block.number)}',
    '${JSON.stringify(baseFees)}',
    to_timestamp('${getBlockTimestamp(block)}')
  )`;

export const storeBaseFeesForBlocks = async (
  analyzedBlocks: { block: BlockLondon; baseFees: BlockBaseFees }[],
): Promise<void> => {
  await sql.unsafe(`
    INSERT INTO base_fees_per_block
      (hash, number, base_fees, mined_at)
    VALUES ${analyzedBlocks.map(toBaseFeeUnsafeInsert).join(",")}
  `);
};

// TODO: because we want to analyze mainnet gas use but don't have baseFeePerGas there we pretend gasUsed is baseFeePerGas there.
export const calcTxrBaseFee = (
  block: BlockLondon,
  txr: TxRWeb3London,
): number =>
  typeof block.baseFeePerGas === "string"
    ? pipe(
        block.baseFeePerGas,
        hexToNumber,
        (baseFeePerGasNum) => baseFeePerGasNum * txr.gasUsed,
      )
    : txr.gasUsed;

/**
 * Map of base fees grouped by contract address
 */
type ContractBaseFeeMap = Record<string, number>;

export const calcBaseFeePerContract = (
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

// Name is undefined because we don't always know the name for a contract. Image is undefined because we don't always have an image for a contract. Address is undefined because base fees paid for ETH transfers are shared between many addresses.
export type BaseFeeBurner = {
  name: string | undefined;
  address: string | undefined;
  image: string | undefined;
  fees: number;
  id: string;
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

  const contractNameMap = await getContractNameMap();

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
      address,
      fees,
      id: address,
      image: undefined,
      name: contractNameMap[address],
    })),
  );

  return pipe(
    [
      {
        address: undefined,
        fees: ethTransferBaseFees,
        id: "eth-transfers",
        image: undefined,
        name: "ETH transfers",
      },
      {
        address: undefined,
        fees: contractCreationBaseFees,
        id: "contract-deployments",
        image: undefined,
        name: "Contract deployments",
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

export const calcBlockBaseFeeSum = (baseFees: BlockBaseFees): number =>
  baseFees.transfers +
  baseFees.contract_creation_fees +
  sum(Object.values(baseFees.contract_use_fees));

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

  return pipe(baseFeesPerBlock, A.map(calcBlockBaseFeeSum), sum);
};

export type FeesBurnedPerDay = Record<string, number>;

export const getFeesBurnedPerDay = async (): Promise<FeesBurnedPerDay> => {
  const blocks = await sql<{ baseFees: BlockBaseFees; minedAt: Date }[]>`
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

  if (blocks.length === 0) {
    return {};
  }

  return pipe(
    blocks,
    NEA.groupBy((block) =>
      pipe(block.minedAt, startOfDay, getUnixTime, String),
    ),
    R.map(
      flow(
        NEA.map((block) => block.baseFees),
        NEA.map(calcBlockBaseFeeSum),
        sum,
      ),
    ),
  );
};

let totalFeesBurned: number | undefined = undefined;

const getRealtimeTotalFeesBurned = async (
  latestBlockBaseFees: BlockBaseFees,
) => {
  if (totalFeesBurned === undefined) {
    totalFeesBurned = await getTotalFeesBurned();
  }

  totalFeesBurned = totalFeesBurned + calcBlockBaseFeeSum(latestBlockBaseFees);
  return totalFeesBurned;
};

// Cache total fees immediately.
getRealtimeTotalFeesBurned({
  contract_use_fees: {},
  contract_creation_fees: 0,
  transfers: 0,
});

export const notifyNewBaseFee = async (
  block: BlockLondon,
  latestBlockBaseFees: BlockBaseFees,
): Promise<void> => {
  // TODO: when running against mainnet pre-london we need to skip some blocks.
  if (block.baseFeePerGas === undefined) {
    return;
  }

  const totalFeesBurned = await getRealtimeTotalFeesBurned(latestBlockBaseFees);

  await sql.notify(
    "base-fee-updates",
    JSON.stringify({
      number: block.number,
      baseFeePerGas: hexToNumber(block.baseFeePerGas),
      totalFeesBurned,
    }),
  );

  return;
};
