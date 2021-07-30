import type { BlockLondon } from "./web3";
import neatCsv from "neat-csv";
import type { Options as NeatCsvOptions } from "neat-csv";
import fs from "fs/promises";
import T from "fp-ts/lib/Task.js";
import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function";
import { sql } from "./db";
import type { BlockBaseFees } from "./base_fees.js";
import { differenceInHours } from "date-fns";
import * as Log from "./log.js";
import * as BaseFees from "./base_fees.js";

type DappAddress = { dapp_id: string; address: string };
type DappAddresses = DappAddress[];
type DappAddressMap = Partial<Record<string, string>>;
let cDappAddressMap: DappAddressMap | undefined = undefined;

const readFile =
  (path: string): T.Task<Buffer> =>
  () =>
    fs.readFile(path);

const readCsv =
  <A>(csv: Buffer, options?: NeatCsvOptions): T.Task<A[]> =>
  () =>
    neatCsv<A>(csv, options);

const buildDappAddressMap = (dappAddresses: DappAddresses) =>
  pipe(
    dappAddresses,
    A.reduce({} as DappAddressMap, (map, { dapp_id, address }) => {
      map[address] = dapp_id;
      return map;
    }),
  );

const getDappAddressMap = async () => {
  if (cDappAddressMap !== undefined) {
    return cDappAddressMap;
  }

  const dappAddressMap = await pipe(
    readFile("./dapp_addresses.csv"),
    T.chain((csv) => readCsv<DappAddress>(csv)),
    T.map(buildDappAddressMap),
    T.map((dappAddressMap) => {
      cDappAddressMap = dappAddressMap;
      return dappAddressMap;
    }),
  )();

  // cache dapp address map.
  cDappAddressMap = dappAddressMap;

  return dappAddressMap;
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
  dappAddressMap: DappAddressMap,
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

export const initDappTotals = async () => {
  const dappAddressMap = await getDappAddressMap();
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
      table: string,
      dappSums: Map<string, string>,
      oldestIncludedBlock: number,
    ) =>
      sql`
      INSERT INTO ${sql(table)}
      ${sql(
        Array.from(dappSums).map(([dappId, feeTotal]) => ({
          dapp_id: dappId,
          contract_address: null,
          fee_total: feeTotal,
          oldest_included_block: oldestIncludedBlock,
        })),
        "dapp_id",
        "contract_address",
        "fee_total",
        "oldest_included_block",
      )}
    `;

    const writeContractSums = (
      table: string,
      contractSums: Map<string, string>,
      oldestIncludedBlock: number,
    ) => sql`
      INSERT INTO ${sql(table)}
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
      table: string,
      dappSums: Map<string, string>,
      contractSums: Map<string, string>,
      oldestIncludedBlock: number,
    ) => {
      await writeDappSums(table, dappSums, oldestIncludedBlock);
      await writeContractSums(table, contractSums, oldestIncludedBlock);
    };

    await writeSums(
      "dapp_24h_totals",
      dappSums24h,
      contractSums24h,
      oldestBlock24h.number,
    );
    await writeSums(
      "dapp_7d_totals",
      dappSums7d,
      contractSums7d,
      oldestBlock7d.number,
    );
    await writeSums(
      "dapp_30d_totals",
      dappSums30d,
      contractSums30d,
      oldestBlock30d.number,
    );
    await writeSums(
      "dapp_totals",
      dappSumsAll,
      contractSumsAll,
      oldestBlockAll.number,
    );
  });

  Log.info("> done inserting totals");
};

export const updateDappTotalsWithFees = async (
  block: BlockLondon,
  baseFees: BlockBaseFees,
) => {
  // load contract address dapp map
  // if the contract is not in our map add new totals under the contract address
  //
};

// periodically update all totals to make sure they don't go stale.
export const updateAllStaleTotals = async () => {};
