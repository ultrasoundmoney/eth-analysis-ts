import neatCsv from "neat-csv";
import type { Options as NeatCsvOptions } from "neat-csv";
import fs from "fs/promises";
import T from "fp-ts/lib/Task.js";
import A from "fp-ts/lib/Array.js";
import { flow, pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import type {
  BaseFeeBurner,
  FeeBreakdown,
  Timeframe as Timeframe,
} from "./base_fees.js";
import { differenceInSeconds } from "date-fns";
import * as Log from "./log.js";
import * as BaseFees from "./base_fees.js";
import NEA from "fp-ts/lib/NonEmptyArray.js";
import R from "fp-ts/lib/Record.js";
import { sum } from "./numbers.js";
import * as eth from "./web3.js";
import type { BlockLondon } from "./web3.js";
import { delay } from "./delay.js";
import * as Transactions from "./transactions.js";
import Sentry from "@sentry/node";

type DappId = string;
type ContractAddress = string;
type DappAddress = { dapp_id: DappId; address: ContractAddress };
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
  baseFees: FeeBreakdown;
  minedAt: Date;
};

type Segments = Record<Timeframe, AnalyzedBlock[]>;

const getSecondsFromDays = (days: number): number => days * 24 * 60 * 60;
const getSecondsFromHours = (hours: number): number => hours * 60 * 60;

const getTimeframeSegments = (blocks: AnalyzedBlock[]): Segments => {
  const now = new Date();
  const blocks1h: AnalyzedBlock[] = [];
  const blocks24h: AnalyzedBlock[] = [];
  const blocks7d: AnalyzedBlock[] = [];
  const blocks30d: AnalyzedBlock[] = [];
  const blocksAll: AnalyzedBlock[] = [];

  blocks.forEach((block) => {
    const secondsAge = differenceInSeconds(now, block.minedAt);

    blocksAll.push(block);

    if (secondsAge < getSecondsFromDays(30)) {
      blocks30d.push(block);
    }

    if (secondsAge < getSecondsFromDays(7)) {
      blocks7d.push(block);
    }

    if (secondsAge < getSecondsFromDays(1)) {
      blocks24h.push(block);
    }

    if (secondsAge < getSecondsFromHours(1)) {
      blocks1h.push(block);
    }
  });

  return {
    "1h": blocks1h,
    "24h": blocks24h,
    "7d": blocks7d,
    "30d": blocks30d,
    all: blocksAll,
  };
};

const groupByDapp = (
  dappAddressMap: AddressToDappMap,
  sumsByContract: Record<string, number>,
): {
  dappSums: Map<DappId, number>;
  contractSums: Map<ContractAddress, number>;
} => {
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

const writeSums = async (
  timeframe: Timeframe,
  sums: Map<string, number>,
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

  let chunksDone = 0;

  // We have more rows to insert than sql parameter substitution will allow. We insert in chunks.
  for (const sumsInsertsChunk of A.chunksOf(10000)(sumsInserts)) {
    await sql`INSERT INTO ${sql(table)} ${sql(sumsInsertsChunk)}`;
    chunksDone += 1;
    Log.debug(
      `done inserting sums chunk ${chunksDone} / ${Math.ceil(
        sumsInserts.length / 10000,
      )}`,
    );
  }

  Log.debug(
    `done writing sums for ${totalType} - ${timeframe}, ${sumsInserts.length} written`,
  );
};

export const calcTotals = async (upToIncludingBlockNumber: number) => {
  const dappAddressMap = await getAddressToDappMap();

  Log.debug(
    `fetching all base fees per block up to and including: ${upToIncludingBlockNumber}`,
  );

  const blocks = await sql<AnalyzedBlock[]>`
      SELECT
        number,
        base_fees,
        mined_at
      FROM base_fees_per_block
      WHERE number <= ${upToIncludingBlockNumber}
      ORDER BY number ASC
    `;

  Log.debug("done fetching all base fees per block");

  const timeframeSegments = getTimeframeSegments(blocks);
  const [oldestBlock1h] = timeframeSegments["1h"];
  const [oldestBlock24h] = timeframeSegments["24h"];
  const [oldestBlock7d] = timeframeSegments["7d"];
  const [oldestBlock30d] = timeframeSegments["30d"];
  const [oldestBlockAll] = timeframeSegments["all"];

  const sumByContract1h = pipe(
    timeframeSegments["1h"],
    A.map((aBlock) => aBlock.baseFees.contract_use_fees),
    BaseFees.sumFeeMaps,
  );

  const sumByContract24h = pipe(
    timeframeSegments["24h"],
    A.map((aBlock) => aBlock.baseFees.contract_use_fees),
    BaseFees.sumFeeMaps,
  );
  const sumByContract7d = pipe(
    timeframeSegments["7d"],
    A.map((aBlock) => aBlock.baseFees.contract_use_fees),
    BaseFees.sumFeeMaps,
  );
  const sumByContract30d = pipe(
    timeframeSegments["30d"],
    A.map((aBlock) => aBlock.baseFees.contract_use_fees),
    BaseFees.sumFeeMaps,
  );
  const sumByContractAll = pipe(
    timeframeSegments["all"],
    A.map((aBlock) => aBlock.baseFees.contract_use_fees),
    BaseFees.sumFeeMaps,
  );

  const { dappSums: dappSums1h, contractSums: contractSums1h } = groupByDapp(
    dappAddressMap,
    sumByContract1h,
  );

  const { dappSums: dappSums24h, contractSums: contractSums24h } = groupByDapp(
    dappAddressMap,
    sumByContract24h,
  );

  const { dappSums: dappSums7d, contractSums: contractSums7d } = groupByDapp(
    dappAddressMap,
    sumByContract7d,
  );

  const { dappSums: dappSums30d, contractSums: contractSums30d } = groupByDapp(
    dappAddressMap,
    sumByContract30d,
  );

  const { dappSums: dappSumsAll, contractSums: contractSumsAll } = groupByDapp(
    dappAddressMap,
    sumByContractAll,
  );

  Log.debug(`found ${dappSumsAll.size} dapps with accumulated base fees`);
  Log.debug(
    `found ${contractSumsAll.size} unknown contracts with accumulated base fees`,
  );

  await ensureContractAddressKnown(Object.keys(sumByContractAll));

  if (oldestBlock1h !== undefined) {
    await sql`TRUNCATE dapp_1h_totals;`;
    await sql`TRUNCATE contract_1h_totals;`;
    await writeSums("1h", dappSums1h, oldestBlock1h.number, "dapp");
    await writeSums("1h", contractSums1h, oldestBlock1h.number, "contract");
  } else {
    Log.warn("no oldest block within 1h found! are we 1h behind?");
  }
  if (oldestBlock24h !== undefined) {
    await sql`TRUNCATE dapp_24h_totals;`;
    await sql`TRUNCATE contract_24h_totals;`;
    await writeSums("24h", dappSums24h, oldestBlock24h.number, "dapp");
    await writeSums("24h", contractSums24h, oldestBlock24h.number, "contract");
  } else {
    Log.warn("no oldest block within 24h found! are we 24h behind?");
  }
  await sql`TRUNCATE dapp_7d_totals;`;
  await sql`TRUNCATE contract_7d_totals;`;
  await writeSums("7d", dappSums7d, oldestBlock7d.number, "dapp");
  await writeSums("7d", contractSums7d, oldestBlock7d.number, "contract");
  await sql`TRUNCATE dapp_30d_totals;`;
  await sql`TRUNCATE contract_30d_totals;`;
  await writeSums("30d", dappSums30d, oldestBlock30d.number, "dapp");
  await writeSums("30d", contractSums30d, oldestBlock30d.number, "contract");
  await sql`TRUNCATE dapp_all_totals;`;
  await sql`TRUNCATE contract_all_totals;`;
  await writeSums("all", dappSumsAll, oldestBlockAll.number, "dapp");
  await writeSums("all", contractSumsAll, oldestBlockAll.number, "contract");

  Log.info("done inserting totals");
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
    writeTotal(block, "1h", dapp, useBaseFee, "dapp"),
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
    writeTotal(block, "1h", address, useBaseFee, "contract"),
    writeTotal(block, "24h", address, useBaseFee, "contract"),
    writeTotal(block, "7d", address, useBaseFee, "contract"),
    writeTotal(block, "30d", address, useBaseFee, "contract"),
    writeTotal(block, "all", address, useBaseFee, "contract"),
  ]).then(() => undefined);

const segmentBaseFeeTotalType = (
  addressToDappMap: AddressToDappMap,
  baseFees: FeeBreakdown,
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
  baseFees: FeeBreakdown,
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
  "1h": 1,
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
  const staleBlocks = await sql<{ number: number; baseFees: FeeBreakdown }[]>`
    SELECT number, base_fees FROM base_fees_per_block
    WHERE now() - mined_at >= interval '${sql(String(maxHours))} hours'
      AND number >= ${oldestIncludedBlock}
    ORDER BY number ASC
  `;

  if (staleBlocks.length === 0) {
    Log.debug(`no stale blocks for ${totalType} - ${id}`);
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
      WHERE dapp_id = ANY (${sql.array(ids)})`;

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
    return;
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
      WHERE contract_address = ANY (${sql.array(ids)})`;

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
  Log.debug(`removing stale fees for ${dappsOrAddresses.length} ${totalType}s`);

  await Promise.all([
    ensureFreshTotal(dappToAdressesMap, "1h", totalType, dappsOrAddresses),
    ensureFreshTotal(dappToAdressesMap, "24h", totalType, dappsOrAddresses),
    ensureFreshTotal(dappToAdressesMap, "7d", totalType, dappsOrAddresses),
    ensureFreshTotal(dappToAdressesMap, "30d", totalType, dappsOrAddresses),
  ]);
};

// type DappName = { dapp_id: string; name: string };
// type DappNameMap = Partial<Record<string, string>>;
// let cDappNameMap: DappNameMap | undefined = undefined;
// const getDappNameMap = async (): Promise<DappNameMap> => {
//   if (cDappNameMap !== undefined) {
//     return cDappNameMap;
//   }

//   return pipe(
//     readFile("./dapp_names.csv"),
//     T.chain((csv) => readCsv<DappName>(csv)),
//     T.map(A.map(({ dapp_id, name }) => [dapp_id, name])),
//     T.map(Object.fromEntries),
//     T.map((dappNameMap) => {
//       cDappNameMap = dappNameMap;
//       return dappNameMap;
//     }),
//   )();
// };

export const getTopFeeBurners = async (
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

  // const tableDapps = getTableName("dapp", timeframe);

  // const dappBurnerCandidatesRaw = await sql<
  //   { dappId: string; feeTotal: BigInt }[]
  // >`
  //   SELECT dapp_id, fee_total FROM ${sql(tableDapps)}
  //   ORDER BY fee_total DESC
  //   LIMIT 11
  // `.then((rows) =>
  //   rows.map((row) => ({ ...row, feeTotal: Number(row.feeTotal) })),
  // );

  // const dappNameMap = await getDappNameMap();

  // const dappBurnerCandidates: BaseFeeBurner[] = dappBurnerCandidatesRaw.map(
  //   ({ dappId, feeTotal }) => ({
  //     fees: feeTotal,
  //     id: dappId,
  //     name: dappNameMap[dappId] || dappId,
  //     image: undefined,
  //   }),
  // );

  const tableContracts = getTableName("contract", timeframe);
  const tableContractsSql = sql(tableContracts);
  const contractBurnerCandidatesRaw = await sql<
    {
      contractAddress: string;
      feeTotal: BigInt;
      name: string | null;
      isBot: boolean;
    }[]
  >`
    SELECT contract_address, fee_total, name, is_bot FROM ${tableContractsSql}
    JOIN contracts
      ON ${tableContractsSql}.contract_address = contracts.address
    ORDER BY fee_total DESC
    LIMIT 12
  `.then((rows) =>
    rows.map((row) => ({ ...row, feeTotal: Number(row.feeTotal) })),
  );

  const contractBurnerCandidates: BaseFeeBurner[] =
    contractBurnerCandidatesRaw.map(
      ({ contractAddress, feeTotal, name, isBot }) => ({
        fees: feeTotal,
        id: contractAddress,
        name: name || contractAddress,
        image: undefined,
        isBot,
      }),
    );

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
      // ...dappBurnerCandidates,
      ...contractBurnerCandidates,
    ],
    A.sort<BaseFeeBurner>({
      compare: (first, second) =>
        Number(first.fees) === Number(second.fees)
          ? 0
          : Number(first.fees) > Number(second.fees)
          ? -1
          : 1,
      equals: (first, second) => Number(first.fees) === Number(second.fees),
    }),
    A.takeLeft(12),
  );
};

export const notifyNewLeaderboard = async (
  block: BlockLondon,
): Promise<void> => {
  const [
    leaderboard1h,
    leaderboard24h,
    leaderboard7d,
    leaderboard30d,
    leaderboardAll,
  ] = await Promise.all([
    getTopFeeBurners("1h"),
    getTopFeeBurners("24h"),
    getTopFeeBurners("7d"),
    getTopFeeBurners("30d"),
    getTopFeeBurners("all"),
  ]);

  await sql.notify(
    "base-fee-updates",
    JSON.stringify({
      type: "leaderboard-update",
      number: block.number,
      leaderboard1h,
      leaderboard24h,
      leaderboard7d,
      leaderboard30d,
      leaderboardAll,
    }),
  );

  await sql.notify(
    "burn-leaderboard-update",
    JSON.stringify({
      number: block.number,
      leaderboard1h,
      leaderboard24h,
      leaderboard7d,
      leaderboard30d,
      leaderboardAll,
    }),
  );

  return;
};

const ensureContractAddressKnown = async (addresses: string[]) => {
  const knownAddresses = await sql<
    { address: string }[]
  >`SELECT address FROM contracts`;
  const knownAddressSet = pipe(
    knownAddresses,
    A.map(({ address }) => address),
    (knownAddresses) => new Set(knownAddresses),
  );

  // Our sql lib thinks we want to insert a string instead of a new row if we don't wrap in object.
  const insertableAddresses = addresses
    .filter((address) => !knownAddressSet.has(address))
    .map((address) => ({ address }));

  // We have more rows to insert than sql parameter substitution will allow. We insert in chunks.
  for (const addressChunk of A.chunksOf(20000)(insertableAddresses)) {
    await sql<{}[]>`
      INSERT INTO contracts
      ${sql(addressChunk, "address")}
      ON CONFLICT DO NOTHING`;
  }

  Log.debug(
    `done ensuring contract addresses are known for ${addresses.length} addresses`,
  );
};

export const watchAndCalcTotalFees = async () => {
  Log.info("starting base fee total analysis");
  const calcTotalsTransaction = Sentry.startTransaction({
    op: "calc-totals",
    name: "calculate totals on start",
  });

  // We can only analyze up to the latest base fee analyzed block. So we check continuously to see if more blocks have been analyzed for fees, and thus fee totals need to be updated.
  const latestBlockNumberAtStart =
    await BaseFees.getLatestAnalyzedBlockNumber();
  if (latestBlockNumberAtStart === undefined) {
    throw new Error("no analyzed blocks, cannot calculate base fee totals");
  }

  Log.debug("calculating base fee totals for all known dapps");
  await calcTotals(latestBlockNumberAtStart);
  Log.debug("done calculating fresh base fee totals");
  calcTotalsTransaction.finish();

  let nextBlockNumberToAnalyze = latestBlockNumberAtStart + 1;

  await eth.webSocketOpen;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const latestAnalyzedBlockNumber =
      await BaseFees.getLatestAnalyzedBlockNumber();

    if (latestAnalyzedBlockNumber === undefined) {
      throw new Error("no analyzed blocks, cannot calculate base fee totals");
    }

    if (nextBlockNumberToAnalyze > latestAnalyzedBlockNumber) {
      // If we've already updated totals for the latest block, wait 2s and try again.
      Log.info("all totals up to date, waiting 2s to check for new block");
      await delay(2000);
      continue;
    }

    const updateTotalsTransaction = Sentry.startTransaction({
      name: "update base fee totals for a new block",
      op: "update-base-fee-totals",
    });

    Log.info(
      `analyzing block ${nextBlockNumberToAnalyze} to update fee totals`,
    );
    const block = await eth.getBlock(nextBlockNumberToAnalyze);
    const txrs = await Transactions.getTxrsWithRetry(block);

    const baseFees = BaseFees.calcBlockFeeBreakdown(block, txrs);
    const addressToDappMap = await getAddressToDappMap();
    const { dappFees, unknownDappFees } = segmentBaseFeeTotalType(
      addressToDappMap,
      baseFees,
    );

    const unknownDappAddresses = unknownDappFees.map(([address]) => address);

    await ensureContractAddressKnown(unknownDappAddresses);

    await Promise.all([
      updateTotalsWithFees(block, baseFees),
      ensureFreshTotals("dapp", Object.keys(dappFees)),
      ensureFreshTotals("contract", Object.keys(unknownDappFees)),
    ]);

    await notifyNewLeaderboard(block);

    nextBlockNumberToAnalyze = nextBlockNumberToAnalyze + 1;

    updateTotalsTransaction.finish();
  }
};
