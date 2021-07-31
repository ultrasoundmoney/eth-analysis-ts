import neatCsv from "neat-csv";
import type { Options as NeatCsvOptions } from "neat-csv";
import fs from "fs/promises";
import T from "fp-ts/lib/Task.js";
import A from "fp-ts/lib/Array.js";
import { flow, pipe } from "fp-ts/lib/function";
import { sql } from "./db";
import type { BlockBaseFees, Timeframe as Timeframe } from "./base_fees.js";
import { differenceInHours } from "date-fns";
import * as Log from "./log.js";
import * as BaseFees from "./base_fees.js";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import R from "fp-ts/lib/Record.js";
import { sum } from "./numbers";
import type { BlockLondon } from "./web3.js";

type DappAddress = { dapp_id: string; address: string };
type AddressToDappMap = Partial<Record<string, string>>;
let cAddressToDappMap: AddressToDappMap | undefined = undefined;

const readFile =
  (path: string): T.Task<Buffer> =>
  () =>
    fs.readFile(path);

const readCsv =
  <A>(csv: Buffer, options?: NeatCsvOptions): T.Task<A[]> =>
  () =>
    neatCsv<A>(csv, options);

const getAddressToDappMap = async () => {
  if (cAddressToDappMap !== undefined) {
    return cAddressToDappMap;
  }

  return pipe(
    readFile("./dapp_addresses.csv"),
    T.chain((csv) => readCsv<DappAddress>(csv)),
    T.map(
      A.reduce({} as AddressToDappMap, (map, { dapp_id, address }) => {
        map[address] = dapp_id;
        return map;
      }),
    ),
    T.map((addressToDappMap) => {
      cAddressToDappMap = addressToDappMap;
      return addressToDappMap;
    }),
  )();
};

type DappToAddressesMap = Record<string, string[]>;
let cDappToAddressesMap: DappToAddressesMap | undefined = undefined;
const getDappToAddressesMap = async () => {
  if (cDappToAddressesMap !== undefined) {
    return cDappToAddressesMap;
  }

  const dappToAddressesMap = await pipe(
    readFile("./dapp_addresses.csv"),
    T.chain((csv) => readCsv<DappAddress>(csv)),
    T.map(NEA.groupBy((dappAddress) => dappAddress.dapp_id)),
    T.map(R.map(A.map((dappAddress) => dappAddress.address))),
    T.map((dappToAdressesMap) => {
      cDappToAddressesMap = dappToAdressesMap;
      return dappToAdressesMap;
    }),
  )();

  cDappToAddressesMap = dappToAddressesMap;

  return dappToAddressesMap;
};

type AnalyzedBlock = {
  number: number;
  baseFees: BlockBaseFees;
  minedAt: Date;
};
type Segments = {
  b24h: AnalyzedBlock[];
  b7d: AnalyzedBlock[];
  b30d: AnalyzedBlock[];
  all: AnalyzedBlock[];
};

const getHoursFromDays = (days: number): number => days * 24;

const getTimeframeSegments = (blocks: AnalyzedBlock[]): Segments => {
  const now = new Date();
  const blocks24h: AnalyzedBlock[] = [];
  const blocks7d: AnalyzedBlock[] = [];
  const blocks30d: AnalyzedBlock[] = [];
  const blocksAll: AnalyzedBlock[] = [];

  blocks.forEach((block) => {
    const hourAge = differenceInHours(now, block.minedAt);

    blocksAll.push(block);

    if (hourAge < getHoursFromDays(30)) {
      blocks30d.push(block);
    }

    if (hourAge < getHoursFromDays(7)) {
      blocks7d.push(block);
    }

    if (hourAge < 24) {
      blocks24h.push(block);
    }
  });

  return {
    b24h: blocks24h,
    b7d: blocks7d,
    b30d: blocks30d,
    all: blocksAll,
  };
};

const groupByDapp = (
  dappAddressMap: AddressToDappMap,
  sumsByContract: Record<string, number>,
) => {
  const dappSums = new Map();
  const contractSums = new Map();

  Object.entries(sumsByContract).forEach(([address, feesForAddress]) => {
    const dapp = dappAddressMap[address];
    if (dapp) {
      const sum = dappSums.get(address) || 0;
      dappSums.set(dapp, sum + feesForAddress);
      return;
    }

    const sum = contractSums.get(address) || 0;
    contractSums.set(address, sum + feesForAddress);
    return;
  });

  return { dappSums, contractSums };
};

export const calcTotals = async () => {
  const dappAddressMap = await getAddressToDappMap();
  await sql.begin(async (sql) => {
    await sql`TRUNCATE dapp_24h_totals;`;
    await sql`TRUNCATE dapp_7d_totals;`;
    await sql`TRUNCATE dapp_30d_totals;`;
    await sql`TRUNCATE dapp_all_totals;`;
    await sql`TRUNCATE contract_24h_totals;`;
    await sql`TRUNCATE contract_7d_totals;`;
    await sql`TRUNCATE contract_30d_totals;`;
    await sql`TRUNCATE contract_all_totals;`;

    const blocks = await sql<AnalyzedBlock[]>`
      SELECT
        number,
        base_fees,
        mined_at
      FROM base_fees_per_block
      ORDER BY number ASC
    `;

    const timeframeSegments = getTimeframeSegments(blocks);
    const oldestBlock24h = timeframeSegments.b24h[0];
    const oldestBlock7d = timeframeSegments.b7d[0];
    const oldestBlock30d = timeframeSegments.b30d[0];
    const oldestBlockAll = timeframeSegments.all[0];

    const sumByContract24h = pipe(
      timeframeSegments.b24h,
      A.map((aBlock) => aBlock.baseFees.contract_use_fees),
      BaseFees.sumFeeMaps,
    );
    const sumByContract7d = pipe(
      timeframeSegments.b7d,
      A.map((aBlock) => aBlock.baseFees.contract_use_fees),
      BaseFees.sumFeeMaps,
    );
    const sumByContract30d = pipe(
      timeframeSegments.b30d,
      A.map((aBlock) => aBlock.baseFees.contract_use_fees),
      BaseFees.sumFeeMaps,
    );
    const sumByContractAll = pipe(
      timeframeSegments.all,
      A.map((aBlock) => aBlock.baseFees.contract_use_fees),
      BaseFees.sumFeeMaps,
    );

    const { dappSums: dappSums24h, contractSums: contractSums24h } =
      groupByDapp(dappAddressMap, sumByContract24h);

    const { dappSums: dappSums7d, contractSums: contractSums7d } = groupByDapp(
      dappAddressMap,
      sumByContract7d,
    );

    const { dappSums: dappSums30d, contractSums: contractSums30d } =
      groupByDapp(dappAddressMap, sumByContract30d);

    const { dappSums: dappSumsAll, contractSums: contractSumsAll } =
      groupByDapp(dappAddressMap, sumByContractAll);

    const writeSums = async (
      timeframe: Timeframe,
      sums: Map<string, string>,
      oldestIncludedBlock: number,
      totalType: TotalType,
    ) => {
      const table = getTableName(totalType, timeframe);
      const idColumn = totalIdColumnMap[totalType];
      const sumsInserts = Array.from(sums).map(([id, feeTotal]) => ({
        [idColumn]: id,
        fee_total: feeTotal,
        oldest_included_block: oldestIncludedBlock,
      }));

      // We have more rows to insert than sql parameter substitution will allow. We insert in chunks.
      for (const sumsInsertsChunk of A.chunksOf(20000)(sumsInserts)) {
        await sql`INSERT INTO ${sql(table)} ${sql(sumsInsertsChunk)}`;
      }
    };

    await writeSums("24h", dappSums24h, oldestBlock24h.number, "dapp");
    await writeSums("7d", dappSums7d, oldestBlock7d.number, "dapp");
    await writeSums("30d", dappSums30d, oldestBlock30d.number, "dapp");
    await writeSums("all", dappSumsAll, oldestBlockAll.number, "dapp");
    await writeSums("24h", contractSums24h, oldestBlock24h.number, "contract");
    await writeSums("7d", contractSums7d, oldestBlock7d.number, "contract");
    await writeSums("30d", contractSums30d, oldestBlock30d.number, "contract");
    await writeSums("all", contractSumsAll, oldestBlockAll.number, "contract");
  });

  Log.info("> done inserting totals");
};

const getTableName = (totalType: TotalType, timeframe: Timeframe) =>
  `${totalType}_${timeframe}_totals`;

export const updateTotalsWithFees = async (
  block: BlockLondon,
  baseFees: BlockBaseFees,
) => {
  const dappAddressMap = await getAddressToDappMap();

  const writeTotal = (
    timeframe: Timeframe,
    id: string,
    useBaseFee: number,
    totalType: TotalType,
  ) => {
    const table = getTableName(totalType, timeframe);
    const idColumn = totalIdColumnMap[totalType];
    return sql`
      INSERT INTO ${sql(table)} AS t (
        ${sql(idColumn)},
        fee_total,
        oldest_included_block
      )
      VALUES (${id}, ${useBaseFee}, ${block.number})
      ON CONFLICT (${sql(idColumn)}) DO UPDATE
        SET fee_total = t.fee_total + ${useBaseFee}`;
  };

  const writeDappTotals = async (dapp: string, useBaseFee: number) => {
    await writeTotal("24h", dapp, useBaseFee, "dapp");
    await writeTotal("7d", dapp, useBaseFee, "dapp");
    await writeTotal("30d", dapp, useBaseFee, "dapp");
    await writeTotal("all", dapp, useBaseFee, "dapp");
  };

  const writeContractTotals = async (address: string, useBaseFee: number) => {
    await writeTotal("24h", address, useBaseFee, "contract");
    await writeTotal("7d", address, useBaseFee, "contract");
    await writeTotal("30d", address, useBaseFee, "contract");
    await writeTotal("all", address, useBaseFee, "contract");
  };

  const useBaseFees = Object.entries(baseFees.contract_use_fees);
  // TODO: handle case of not seen before contract
  for (const [address, useBaseFee] of useBaseFees) {
    const dapp = dappAddressMap[address];
    if (dapp) {
      await writeDappTotals(dapp, useBaseFee);
      await ensureFreshTotals("dapp", dapp);
      // await subtractOldestIncludedBlock("dapp_id", dapp);
    } else {
      await writeContractTotals(address, useBaseFee);
      await ensureFreshTotals("contract", address);
      // await subtractOldestIncludedBlock("contract_address", address);
    }
  }
};

type TotalType = "dapp" | "contract";
const totalIdColumnMap: Record<TotalType, string> = {
  contract: "contract_address",
  dapp: "dapp_id",
};

const timeframeHoursMap: Record<Timeframe, number> = {
  "24h": 24,
  "7d": 24 * 7,
  "30d": 24 * 30,
  all: Number.POSITIVE_INFINITY,
};

const subtractStaleBaseFees = async (
  dappToAdressesMap: DappToAddressesMap,
  timeframe: Timeframe,
  oldestIncludedBlock: number,
  totalType: TotalType,
  id: string,
) => {
  const table = getTableName(totalType, timeframe);
  const maxHours = timeframeHoursMap[timeframe];
  const staleBlocks = await sql<{ number: number; baseFees: BlockBaseFees }[]>`
    SELECT number, base_fees FROM base_fees_per_block
    WHERE now() - mined_at >= interval '${sql(String(maxHours))} hours'
      AND number >= ${oldestIncludedBlock}
    ORDER BY number ASC
  `;

  if (staleBlocks.length === 0) {
    Log.debug(`> no stale blocks for ${totalType} - ${id}`);
    return;
  }

  const { number: oldestFreshBlockNumber } = staleBlocks[0];
  const addresses = totalType === "contract" ? [id] : dappToAdressesMap[id];
  const staleSum = pipe(
    staleBlocks,
    A.map(
      flow(
        (block) =>
          addresses.map((address) => block.baseFees.contract_use_fees[address]),
        sum,
      ),
    ),
    sum,
  );

  await sql`
      UPDATE ${sql(table)}
      SET fee_total = fee_total - ${staleSum}
      SET oldest_included_block = ${oldestFreshBlockNumber}
      WHERE ${sql(totalIdColumnMap[totalType])} = ${id}`;
};

const ensureFreshTotal = async (
  dappToAdressesMap: DappToAddressesMap,
  timeframe: Timeframe,
  totalType: TotalType,
  id: string,
) => {
  Log.debug(`> ensuring freshness for ${totalType} - ${id}`);
  const table = getTableName(totalType, timeframe);
  const idColumn = totalIdColumnMap[totalType];
  const [dappTotal] = await sql<
    {
      oldestIncludedBlock: number;
      feeTotal: number;
    }[]
  >`
    SELECT oldest_included_block, fee_total
    FROM ${sql(table)}
    JOIN base_fees_per_block ON oldest_included_block = number
    WHERE ${sql(idColumn)} = ${id}`;

  if (dappTotal === undefined) {
    throw new Error(`> missing dapp total for ${totalType} ${id}`);
  }

  Log.debug(`> current fee total is ${dappTotal.feeTotal}`);

  await subtractStaleBaseFees(
    dappToAdressesMap,
    timeframe,
    dappTotal.oldestIncludedBlock,
    totalType,
    id,
  );

  const [newTotal] = await sql<{ feeTotal: number }[]>`
    SELECT fee_total
    FROM ${sql(table)}
    WHERE ${sql(totalIdColumnMap[totalType])} = ${id}`;

  Log.debug(`> fee total after stale removal ${newTotal.feeTotal}`);
};

const ensureFreshTotals = async (totalType: TotalType, id: string) => {
  const dappToAdressesMap = await getDappToAddressesMap();

  await ensureFreshTotal(dappToAdressesMap, "24h", totalType, id);
  await ensureFreshTotal(dappToAdressesMap, "7d", totalType, id);
  await ensureFreshTotal(dappToAdressesMap, "30d", totalType, id);
};

// periodically update all totals to make sure they don't go stale.
export const updateAllStaleTotals = async () => {};
