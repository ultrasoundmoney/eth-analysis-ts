import neatCsv from "neat-csv";
import type { Options as NeatCsvOptions } from "neat-csv";
import fs from "fs/promises";
import T from "fp-ts/lib/Task.js";
import A from "fp-ts/lib/Array.js";
import { flow, pipe } from "fp-ts/lib/function";
import { sql } from "./db";
import type {
  BaseFeeBurner,
  BlockBaseFees,
  Timeframe as Timeframe,
} from "./base_fees.js";
import { differenceInHours } from "date-fns";
import * as Log from "./log.js";
import * as BaseFees from "./base_fees.js";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import R from "fp-ts/lib/Record.js";
import { sum } from "./numbers";
import * as eth from "./web3.js";
import type { BlockLondon } from "./web3.js";
import Config from "./config.js";
import { delay } from "./delay";

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
    const [oldestBlock24h] = timeframeSegments.b24h;
    const [oldestBlock7d] = timeframeSegments.b7d;
    const [oldestBlock30d] = timeframeSegments.b30d;
    const [oldestBlockAll] = timeframeSegments.all;

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

    Log.debug(`> found ${dappSumsAll.size} dapps with accumulated base fees`);
    Log.debug(
      `> found ${contractSumsAll.size} unknown contracts with accumulated base fees`,
    );

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

    if (oldestBlock24h !== undefined) {
      await writeSums("24h", dappSums24h, oldestBlock24h.number, "dapp");
      await writeSums(
        "24h",
        contractSums24h,
        oldestBlock24h.number,
        "contract",
      );
    } else {
      Log.warn("no oldest block within 24h found! are we 24h behind?");
    }
    await writeSums("7d", dappSums7d, oldestBlock7d.number, "dapp");
    await writeSums("7d", contractSums7d, oldestBlock7d.number, "contract");
    await writeSums("30d", dappSums30d, oldestBlock30d.number, "dapp");
    await writeSums("30d", contractSums30d, oldestBlock30d.number, "contract");
    await writeSums("all", dappSumsAll, oldestBlockAll.number, "dapp");
    await writeSums("all", contractSumsAll, oldestBlockAll.number, "contract");
  });

  Log.info("> done inserting totals");
};

const getTableName = (totalType: TotalType, timeframe: Timeframe) =>
  `${totalType}_${timeframe}_totals`;

const writeTotal = async (
  block: BlockLondon,
  timeframe: Timeframe,
  id: string,
  useBaseFee: number,
  totalType: TotalType,
): Promise<void> => {
  const table = getTableName(totalType, timeframe);
  const idColumn = totalIdColumnMap[totalType];
  await sql`
      INSERT INTO ${sql(table)} AS t (
        ${sql(idColumn)},
        fee_total,
        oldest_included_block
      )
      VALUES (${id}, ${useBaseFee}, ${block.number})
      ON CONFLICT (${sql(idColumn)}) DO UPDATE
        SET fee_total = t.fee_total + ${useBaseFee}`;
  return undefined;
};

const writeDappTotals = async (
  block: BlockLondon,
  dapp: string,
  useBaseFee: number,
): Promise<void> =>
  Promise.all([
    writeTotal(block, "24h", dapp, useBaseFee, "dapp"),
    writeTotal(block, "7d", dapp, useBaseFee, "dapp"),
    writeTotal(block, "30d", dapp, useBaseFee, "dapp"),
    writeTotal(block, "all", dapp, useBaseFee, "dapp"),
  ]).then(() => undefined);

const writeContractTotals = async (
  block: BlockLondon,
  address: string,
  useBaseFee: number,
): Promise<void> =>
  Promise.all([
    writeTotal(block, "24h", address, useBaseFee, "contract"),
    writeTotal(block, "7d", address, useBaseFee, "contract"),
    writeTotal(block, "30d", address, useBaseFee, "contract"),
    writeTotal(block, "all", address, useBaseFee, "contract"),
  ]).then(() => undefined);

const segmentBaseFeeTotalType = (
  addressToDappMap: AddressToDappMap,
  baseFees: BlockBaseFees,
): { dappFees: [string, number][]; unknownDappFees: [string, number][] } => {
  const useBaseFees = Object.entries(baseFees.contract_use_fees) as [
    string,
    number,
  ][];

  const dappFees = useBaseFees
    .filter(([address]) => addressToDappMap[address] !== undefined)
    .map(([address, useBaseFees]): [string, number] => [
      addressToDappMap[address]!,
      useBaseFees,
    ]);
  const unknownDappFees = useBaseFees.filter(
    ([address]) => addressToDappMap[address] === undefined,
  );

  return { dappFees, unknownDappFees };
};

export const updateTotalsWithFees = async (
  block: BlockLondon,
  baseFees: BlockBaseFees,
) => {
  const addressToDappMap = await getAddressToDappMap();

  const { dappFees, unknownDappFees } = segmentBaseFeeTotalType(
    addressToDappMap,
    baseFees,
  );

  await Promise.all([
    ...dappFees.map(([dapp, fee]) => writeDappTotals(block, dapp, fee)),
    ...unknownDappFees.map(([address, fee]) =>
      writeContractTotals(block, address, fee),
    ),
    ensureFreshTotals("dapp", Object.keys(dappFees)),
    ensureFreshTotals("contract", Object.keys(unknownDappFees)),
  ]);
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
          addresses.map(
            (address) => block.baseFees.contract_use_fees[address] || 0,
          ),
        sum,
      ),
    ),
    sum,
  );

  await sql`
    UPDATE ${sql(table)}
    SET
      fee_total = fee_total - ${staleSum},
      oldest_included_block = ${oldestFreshBlockNumber}
    WHERE ${sql(totalIdColumnMap[totalType])} = ${id}`;
};

const ensureFreshTotal = async (
  dappToAdressesMap: DappToAddressesMap,
  timeframe: Timeframe,
  totalType: TotalType,
  ids: string[],
): Promise<void> => {
  const table = getTableName(totalType, timeframe);
  if (totalType === "dapp") {
    const dappTotals = await sql<
      {
        dappId: string;
        oldestIncludedBlock: number;
      }[]
    >`
      SELECT oldest_included_block, dapp_id
      FROM ${sql(table)}
      JOIN base_fees_per_block ON oldest_included_block = number
      WHERE dapp_id = ANY ${sql.array(ids)}`;

    Log.debug(`> removing stale fees for ${ids.length} dapps`);

    await Promise.all(
      dappTotals.map((dappTotal) =>
        subtractStaleBaseFees(
          dappToAdressesMap,
          timeframe,
          dappTotal.oldestIncludedBlock,
          "dapp",
          dappTotal.dappId,
        ),
      ),
    );
  }

  const contractTotals = await sql<
    {
      contractAddress: string;
      oldestIncludedBlock: number;
    }[]
  >`
      SELECT oldest_included_block, contract_address
      FROM ${sql(table)}
      JOIN base_fees_per_block ON oldest_included_block = number
      WHERE contract_address = ANY ${sql.array(ids)}`;

  Log.debug(`> removing stale fees for ${ids.length} contracts`);

  await Promise.all(
    contractTotals.map((contractTotal) =>
      subtractStaleBaseFees(
        dappToAdressesMap,
        timeframe,
        contractTotal.oldestIncludedBlock,
        "contract",
        contractTotal.contractAddress,
      ),
    ),
  );
};

const ensureFreshTotals = async (
  totalType: TotalType,
  dappsOrAddresses: string[],
) => {
  const dappToAdressesMap = await getDappToAddressesMap();

  await Promise.all([
    ensureFreshTotal(dappToAdressesMap, "24h", totalType, dappsOrAddresses),
    ensureFreshTotal(dappToAdressesMap, "7d", totalType, dappsOrAddresses),
    ensureFreshTotal(dappToAdressesMap, "30d", totalType, dappsOrAddresses),
  ]);
};

type DappName = { dapp_id: string; name: string };
type DappNameMap = Partial<Record<string, string>>;
let cDappNameMap: DappNameMap | undefined = undefined;
const getDappNameMap = async (): Promise<DappNameMap> => {
  if (cDappNameMap !== undefined) {
    return cDappNameMap;
  }

  return pipe(
    readFile("./dapp_names.csv"),
    T.chain((csv) => readCsv<DappName>(csv)),
    T.map(A.map(({ dapp_id, name }) => [dapp_id, name])),
    T.map(Object.fromEntries),
    T.map((dappNameMap) => {
      cDappNameMap = dappNameMap;
      return dappNameMap;
    }),
  )();
};

export const getTopTenFeeBurners = async (
  timeframe: Timeframe,
): Promise<BaseFeeBurner[]> => {
  // const maxHours = timeframeHoursMap[timeframe];
  // const baseFeesPerBlock = await sql<{ baseFees: BlockBaseFees }[]>`
  //     SELECT base_fees
  //     FROM base_fees_per_block
  //     WHERE now() - mined_at >= interval '${sql(String(maxHours))} hours'
  // `.then((rows) => {
  //   if (rows.length === 0) {
  //     Log.warn(
  //       "tried to determine top fee burners but found no analyzed blocks",
  //     );
  //     return [];
  //   }

  //   return rows.map((row) => row.baseFees);
  // });

  // const ethTransferBaseFees = pipe(
  //   baseFeesPerBlock,
  //   A.map((baseFees) => baseFees.transfers),
  //   sum,
  // );
  // const contractCreationBaseFees = pipe(
  //   baseFeesPerBlock,
  //   A.map((baseFees) => baseFees.contract_creation_fees),
  //   sum,
  // );

  const tableDapps = getTableName("dapp", timeframe);
  const tableContracts = getTableName("contract", timeframe);

  const dappBurnerCandidatesRaw = await sql<
    { dappId: string; feeTotal: number }[]
  >`
    SELECT dapp_id, fee_total FROM ${sql(tableDapps)}
    ORDER BY fee_total DESC
    LIMIT 10
  `;
  Log.debug("> dapp query done");

  const dappNameMap = await getDappNameMap();
  const missingDappNames: string[] = [];
  const getDappName = (dappId: string): string => {
    const dappName = dappNameMap[dappId];
    if (dappName === undefined) {
      missingDappNames.push(dappId);
      return dappId;
    }

    return dappName;
  };

  const dappBurnerCandidates: BaseFeeBurner[] = dappBurnerCandidatesRaw.map(
    ({ dappId, feeTotal }) => ({
      fees: feeTotal,
      id: dappId,
      name: getDappName(dappId),
      image: undefined,
    }),
  );

  const contractBurnerCandidatesRaw = await sql<
    { contractAddress: string; feeTotal: number }[]
  >`
    SELECT contract_address, fee_total FROM ${sql(tableContracts)}
    ORDER BY fee_total DESC
    LIMIT 10
  `;
  Log.debug("> contract query done");
  const contractBurnerCandidates: BaseFeeBurner[] =
    contractBurnerCandidatesRaw.map(({ contractAddress, feeTotal }) => ({
      fees: feeTotal,
      id: contractAddress,
      name: contractAddress,
      image: undefined,
    }));

  fs.appendFile("./missingDappNames.txt", missingDappNames.join("\n"));

  return pipe(
    [
      // {
      //   fees: ethTransferBaseFees,
      //   id: "eth-transfers",
      //   image: undefined,
      //   name: "ETH transfers",
      // },
      // {
      //   fees: contractCreationBaseFees,
      //   id: "contract-deployments",
      //   image: undefined,
      //   name: "Contract deployments",
      // },
      ...dappBurnerCandidates,
      ...contractBurnerCandidates,
    ],
    A.sort<BaseFeeBurner>({
      compare: (first, second) =>
        first.fees === second.fees ? 0 : first.fees > second.fees ? -1 : 1,
      equals: (first, second) => first.fees === second.fees,
    }),
    A.takeLeft(10),
  );
};

export const notifyNewLeaderboard = async (): Promise<void> => {
  const [leaderboard24h, leaderboard7d, leaderboard30d, leaderboardAll] =
    await Promise.all([
      getTopTenFeeBurners("24h"),
      getTopTenFeeBurners("7d"),
      getTopTenFeeBurners("30d"),
      getTopTenFeeBurners("all"),
    ]);

  await sql.notify(
    "base-fee-updates",
    JSON.stringify({
      type: "leaderboard-update",
      leaderboard24h,
      leaderboard7d,
      leaderboard30d,
      leaderboardAll,
    }),
  );

  return;
};

export const watchAndAnalyzeBlocks = async () => {
  Log.info("> starting base fee total analysis");
  Log.info(`> chain: ${Config.chain}`);

  Log.debug("> calculating base fee totals for all known dapps");
  await calcTotals();
  Log.debug("> done calculating fresh base fee totals");

  let latestAnalyzedBlockNumber = await BaseFees.getLatestAnalyzedBlockNumber();

  if (latestAnalyzedBlockNumber === undefined) {
    throw new Error("> no analyzed blocks, cannot calculate base fee totals");
  }

  await eth.webSocketOpen;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const latestBlock = await eth.getBlock("latest");

    if (latestBlock.number === latestAnalyzedBlockNumber) {
      // if we've already updated totals for the latest block, wait 2s and try again.
      await delay(2000);
      continue;
    }

    // Next block to analyze
    const blockNumber = latestAnalyzedBlockNumber + 1;
    const block = await eth.getBlock(blockNumber);

    const baseFees = await BaseFees.calcBlockBaseFees(block);
    const addressToDappMap = await getAddressToDappMap();
    const { dappFees, unknownDappFees } = segmentBaseFeeTotalType(
      addressToDappMap,
      baseFees,
    );

    await Promise.all([
      updateTotalsWithFees(block, baseFees),
      ensureFreshTotals("dapp", Object.keys(dappFees)),
      ensureFreshTotals("contract", Object.keys(unknownDappFees)),
    ]);

    latestAnalyzedBlockNumber = latestAnalyzedBlockNumber + 1;
  }
};
