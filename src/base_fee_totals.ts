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
import { sum } from "./numbers.js";
import * as eth from "./web3.js";
import type { BlockLondon } from "./web3.js";
import { delay } from "./delay.js";
import * as Transactions from "./transactions.js";
import Sentry from "@sentry/node";

type ContractAddress = string;

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

const writeSums = async (
  timeframe: Timeframe,
  sums: Record<ContractAddress, number>,
  oldestIncludedBlock: number,
) => {
  const table = getTableName(timeframe);
  const sumsInserts = Object.entries(sums).map(([id, feeTotal]) => ({
    contract_address: id,
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
    `done writing sums for contract - ${timeframe}, ${sumsInserts.length} written`,
  );
};

export const calcTotals = async (upToIncludingBlockNumber: number) => {
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

  const contractAddressesAll = Object.keys(sumByContractAll);
  Log.debug(
    `found ${contractAddressesAll.length} contracts with accumulated base fees`,
  );

  await ensureContractAddressKnown(contractAddressesAll);

  if (oldestBlock1h !== undefined) {
    await sql`TRUNCATE contract_1h_totals;`;
    await writeSums("1h", sumByContract1h, oldestBlock1h.number);
  } else {
    Log.warn("no oldest block within 1h found! are we 1h behind?");
  }
  if (oldestBlock24h !== undefined) {
    await sql`TRUNCATE contract_24h_totals;`;
    await writeSums("24h", sumByContract24h, oldestBlock24h.number);
  } else {
    Log.warn("no oldest block within 24h found! are we 24h behind?");
  }
  await sql`TRUNCATE contract_7d_totals;`;
  await writeSums("7d", sumByContract7d, oldestBlock7d.number);
  await sql`TRUNCATE contract_30d_totals;`;
  await writeSums("30d", sumByContract30d, oldestBlock30d.number);
  await sql`TRUNCATE contract_all_totals;`;
  await writeSums("all", sumByContractAll, oldestBlockAll.number);

  Log.info("done inserting totals");
};

const getTableName = (timeframe: Timeframe) => `contract_${timeframe}_totals`;

const writeTotal = async (
  block: BlockLondon,
  timeframe: Timeframe,
  id: string,
  useBaseFee: number,
): Promise<void> => {
  const table = getTableName(timeframe);
  await sql`
      INSERT INTO ${sql(table)} AS t (
        contract_address,
        fee_total,
        oldest_included_block
      )
      VALUES (${id}, ${useBaseFee}, ${block.number})
      ON CONFLICT (contract_address) DO UPDATE
        SET fee_total = t.fee_total + ${useBaseFee}`;
  return undefined;
};

const writeContractTotals = async (
  block: BlockLondon,
  address: string,
  useBaseFee: number,
): Promise<void> =>
  Promise.all([
    writeTotal(block, "1h", address, useBaseFee),
    writeTotal(block, "24h", address, useBaseFee),
    writeTotal(block, "7d", address, useBaseFee),
    writeTotal(block, "30d", address, useBaseFee),
    writeTotal(block, "all", address, useBaseFee),
  ]).then(() => undefined);

export const updateTotalsWithFees = async (
  block: BlockLondon,
  baseFees: FeeBreakdown,
) => {
  const unknownDappFees = baseFees;

  await Promise.all(
    Object.entries(unknownDappFees.contract_use_fees).map(([address, fee]) =>
      writeContractTotals(block, address, fee!),
    ),
  );
  await ensureFreshTotals(Object.keys(unknownDappFees));
};

const timeframeHoursMap: Record<Timeframe, number> = {
  "1h": 1,
  "24h": 24,
  "7d": 24 * 7,
  "30d": 24 * 30,
  all: Number.POSITIVE_INFINITY,
};

const subtractStaleBaseFees = async (
  timeframe: Timeframe,
  oldestIncludedBlock: number,
  id: string,
) => {
  const table = getTableName(timeframe);
  const maxHours = timeframeHoursMap[timeframe];
  const staleBlocks = await sql<{ number: number; baseFees: FeeBreakdown }[]>`
    SELECT number, base_fees FROM base_fees_per_block
    WHERE now() - mined_at >= interval '${sql(String(maxHours))} hours'
      AND number >= ${oldestIncludedBlock}
    ORDER BY number ASC
  `;

  if (staleBlocks.length === 0) {
    Log.debug(`no stale blocks for contract - ${id}`);
    return;
  }

  const { number: oldestFreshBlockNumber } = staleBlocks[0];
  const addresses = [id];
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
    WHERE contract_address = ${id}`;
};

const ensureFreshTotal = async (
  timeframe: Timeframe,
  ids: string[],
): Promise<void> => {
  const table = getTableName(timeframe);

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
        timeframe,
        contractTotal.oldestIncludedBlock,
        contractTotal.contractAddress,
      ),
    ),
  );
};

const ensureFreshTotals = async (addresses: string[]) => {
  Log.debug(`removing stale fees for ${addresses.length} contracts`);

  await Promise.all([
    ensureFreshTotal("1h", addresses),
    ensureFreshTotal("24h", addresses),
    ensureFreshTotal("7d", addresses),
    ensureFreshTotal("30d", addresses),
  ]);
};

export const getTopFeeBurners = async (
  timeframe: Timeframe,
): Promise<BaseFeeBurner[]> => {
  const tableContracts = getTableName(timeframe);
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
    `ensuring db contract entities exist for ${addresses.length} addresses with base fees`,
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

  Log.debug("calculating base fee totals for all known contracts");
  await calcTotals(latestBlockNumberAtStart);
  Log.debug("done calculating fresh base fee totals");
  calcTotalsTransaction.finish();

  let nextBlockNumberToAnalyze = latestBlockNumberAtStart + 1;

  await eth.webSocketOpen;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const latestAnalyzedBlockNumber =
      await BaseFees.getLatestAnalyzedBlockNumber();
    Log.debug(`latest analyzed block number: ${latestAnalyzedBlockNumber}`);

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
    const contractAddresses = Object.keys(baseFees.contract_use_fees);

    await ensureContractAddressKnown(contractAddresses);
    await updateTotalsWithFees(block, baseFees);
    await ensureFreshTotals(contractAddresses);

    await notifyNewLeaderboard(block);

    nextBlockNumberToAnalyze = nextBlockNumberToAnalyze + 1;

    updateTotalsTransaction.finish();
  }
};
