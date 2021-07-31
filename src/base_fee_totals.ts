import neatCsv from "neat-csv";
import type { Options as NeatCsvOptions } from "neat-csv";
import fs from "fs/promises";
import T from "fp-ts/lib/Task.js";
import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function";
import { sql } from "./db";
import type { BlockBaseFees, Timeframe as Timeframe } from "./base_fees.js";
import { differenceInHours } from "date-fns";
import * as Log from "./log.js";
import * as BaseFees from "./base_fees.js";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import R from "fp-ts/lib/Record.js";
import { sum } from "./numbers";

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

type DappToAddressesMap = Partial<Record<string, string[]>>;
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
    await sql`TRUNCATE dapp_totals;`;

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

    const writeDappSums = (
      timeframe: Timeframe,
      dappSums: Map<string, string>,
      oldestIncludedBlock: number,
    ) =>
      sql`
      INSERT INTO ${sql(timeframeTableMap[timeframe])}
      ${sql(
        Array.from(dappSums).map(([dappId, feeTotal]) => ({
          dapp_id: dappId,
          contract_address: null,
          fee_total: feeTotal,
          oldest_included_block: oldestIncludedBlock,
        })),
      )}
    `;

    const writeContractSums = (
      timeframe: Timeframe,
      contractSums: Map<string, string>,
      oldestIncludedBlock: number,
    ) => sql`
      INSERT INTO ${sql(timeframeTableMap[timeframe])}
      ${sql(
        Array.from(contractSums).map(([address, feeTotal]) => ({
          dapp_id: null,
          contract_address: address,
          fee_total: feeTotal,
          oldest_included_block: oldestIncludedBlock,
        })),
      )}
    `;

    const writeSums = async (
      timeframe: Timeframe,
      dappSums: Map<string, string>,
      contractSums: Map<string, string>,
      oldestIncludedBlock: number,
    ) => {
      await writeDappSums(timeframe, dappSums, oldestIncludedBlock);
      await writeContractSums(timeframe, contractSums, oldestIncludedBlock);
    };

    await writeSums("24h", dappSums24h, contractSums24h, oldestBlock24h.number);
    await writeSums("7d", dappSums7d, contractSums7d, oldestBlock7d.number);
    await writeSums("30d", dappSums30d, contractSums30d, oldestBlock30d.number);
    await writeSums("all", dappSumsAll, contractSumsAll, oldestBlockAll.number);
  });

  Log.info("> done inserting totals");
};

const timeframeTableMap: Record<Timeframe, string> = {
  "24h": "dapp_24h_totals",
  "7d": "dapp_7d_totals",
  "30d": "dapp_30d_totals",
  all: "dapp_totals",
};

export const updateTotalsWithFees = async (baseFees: BlockBaseFees) => {
  const dappAddressMap = await getAddressToDappMap();

  const writeTotal = (
    timeframe: Timeframe,
    dapp: string,
    feeTotal: number,
    idType: DappTotalId,
  ) => sql`
    UPDATE ${sql(timeframeTableMap[timeframe])}
    SET ${sql({ fee_total: feeTotal }, "fee_total")}
    WHERE ${sql(idType)} = ${dapp}`;

  const writeDappTotals = async (dapp: string, feeTotal: number) => {
    await writeTotal("24h", dapp, feeTotal, "dapp_id");
    await writeTotal("7d", dapp, feeTotal, "dapp_id");
    await writeTotal("30d", dapp, feeTotal, "dapp_id");
    await writeTotal("all", dapp, feeTotal, "dapp_id");
  };

  const writeContractTotals = async (address: string, feeTotal: number) => {
    await writeTotal("24h", address, feeTotal, "contract_address");
    await writeTotal("7d", address, feeTotal, "contract_address");
    await writeTotal("30d", address, feeTotal, "contract_address");
    await writeTotal("all", address, feeTotal, "contract_address");
  };

  const useBaseFees = Object.entries(baseFees.contract_use_fees);
  for (const [address, feeTotal] of useBaseFees) {
    const dapp = dappAddressMap[address];
    if (dapp) {
      await writeDappTotals(dapp, feeTotal);
      await ensureFreshTotals("dapp_id", dapp);
    } else {
      await writeContractTotals(address, feeTotal);
      await ensureFreshTotals("contract_address", address);
    }
  }
};

type DappTotalId = "dapp_id" | "contract_address";

const timeframeHoursMap: Record<Timeframe, number> = {
  "24h": 24,
  "7d": 24 * 7,
  "30d": 24 * 30,
  all: Number.POSITIVE_INFINITY,
};

const ensureFreshTotal = async (
  timeframe: Timeframe,
  type: DappTotalId,
  id: string,
) => {
  const table = timeframeTableMap[timeframe];
  const { oldestIncludedBlock, minedAt, baseFees, feeTotal } = await sql<
    {
      oldestIncludedBlock: number;
      minedAt: Date;
      baseFees: BlockBaseFees;
      feeTotal: number;
    }[]
  >`
    SELECT oldest_included_block, mined_at, base_fees, fee_total
    FROM ${sql(table)}
    JOIN base_fees_per_block ON oldest_included_block = number
    WHERE ${sql(type)} = ${id}`.then((rows) => rows[0]);

  const dappToAdressesMap = await getDappToAddressesMap();
  const now = new Date();
  const oldestBlockHoursAge = differenceInHours(now, minedAt);
  const maxHourAge = timeframeHoursMap[timeframe];
  if (oldestBlockHoursAge > maxHourAge) {
    if (type === "dapp_id") {
      const contracts = dappToAdressesMap[id];
      if (contracts === undefined) {
        throw new Error(
          "> tried to ensure freshnesh for dapp but no adresses found",
        );
      }
      const staleFees = pipe(
        contracts,
        A.map((address) => baseFees.contract_use_fees[address]),
        sum,
      );

      await sql`
        UPDATE ${sql(table)}
        SET
          fee_total = ${feeTotal - staleFees},
          oldest_included_block = ${oldestIncludedBlock + 1}
        WHERE ${sql(type)} = ${id}
      `;
    } else {
      const staleFees = baseFees.contract_use_fees[id];

      await sql`
        UPDATE ${sql(table)}
        SET
          fee_total = ${feeTotal - staleFees},
          oldest_included_block = ${oldestIncludedBlock + 1}
        WHERE ${sql(type)} = ${id}
      `;
    }
  }

  console.log(oldestIncludedBlock, minedAt);
};

const ensureFreshTotals = async (type: DappTotalId, id: string) => {
  await ensureFreshTotal("24h", type, id);
  await ensureFreshTotal("7d", type, id);
  await ensureFreshTotal("30d", type, id);
};

// periodically update all totals to make sure they don't go stale.
export const updateAllStaleTotals = async () => {};
