import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import type {
  BaseFeeBurner,
  FeeBreakdown,
  LimitedTimeframe,
  Timeframe as Timeframe,
} from "./base_fees.js";
import { differenceInSeconds } from "date-fns";
import * as Log from "./log.js";
import * as BaseFees from "./base_fees.js";
import * as EthNode from "./eth_node.js";
import type { BlockLondon } from "./eth_node.js";
import { delay } from "./delay.js";
import * as Transactions from "./transactions.js";
import Sentry from "@sentry/node";
import * as Blocks from "./blocks.js";

type ContractAddress = string;

type AnalyzedBlock = {
  number: number;
  baseFees: FeeBreakdown;
  minedAt: Date;
};

const getSecondsFromDays = (days: number): number => days * 24 * 60 * 60;
const getSecondsFromHours = (hours: number): number => hours * 60 * 60;

const timeframeInSecondsMap: Record<LimitedTimeframe, number> = {
  "1h": getSecondsFromHours(1),
  "24h": getSecondsFromHours(24),
  "7d": getSecondsFromDays(7),
  "30d": getSecondsFromDays(30),
};

const getBlocksWithinTimeframe = (
  timeframe: Timeframe,
  blocks: AnalyzedBlock[],
): AnalyzedBlock[] => {
  if (timeframe === "all") {
    return blocks;
  }

  const now = new Date();
  const getSecondsAge = (dt: Date) => differenceInSeconds(now, dt);
  const maxAge = timeframeInSecondsMap[timeframe];

  return pipe(
    blocks,
    A.filter((block) => getSecondsAge(block.minedAt) < maxAge),
  );
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
      `done inserting sums chunk ${timeframe} ${chunksDone} / ${Math.ceil(
        sumsInserts.length / 10000,
      )}`,
    );
  }

  Log.debug(
    `done writing sums for contract - ${timeframe}, ${sumsInserts.length} written`,
  );
};

const calcTotalForTimeframe = async (
  timeframe: Timeframe,
  blocks: AnalyzedBlock[],
): Promise<void> => {
  const blocksWithinTimeframe = getBlocksWithinTimeframe(timeframe, blocks);
  const [oldestBlock] = blocksWithinTimeframe;
  const sums = pipe(
    blocksWithinTimeframe,
    A.map((aBlock) => aBlock.baseFees.contract_use_fees),
    BaseFees.sumFeeMaps,
  );
  const table = getTableName(timeframe);
  await sql`TRUNCATE ${sql(table)};`;
  await writeSums(timeframe, sums, oldestBlock.number);
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

  const contractAddressesAll = pipe(
    blocks,
    A.map((block) => block.baseFees.contract_use_fees),
    A.map(Object.keys),
    A.flatten,
  );

  Log.debug(
    `found ${contractAddressesAll.length} contracts with accumulated base fees`,
  );

  await ensureContractAddressKnown(contractAddressesAll);

  await Promise.all([
    calcTotalForTimeframe("1h", blocks),
    calcTotalForTimeframe("24h", blocks),
    calcTotalForTimeframe("7d", blocks),
    calcTotalForTimeframe("30d", blocks),
    calcTotalForTimeframe("all", blocks),
  ]);

  Log.info("done inserting totals");
};

const getTableName = (timeframe: Timeframe) => `contract_${timeframe}_totals`;

const addFeeToContractForTimeframe = async (
  block: BlockLondon,
  timeframe: Timeframe,
  address: string,
  useBaseFee: number,
): Promise<void> => {
  const table = getTableName(timeframe);
  await sql`
      INSERT INTO ${sql(table)} AS t (
        contract_address,
        fee_total,
        oldest_included_block
      )
      VALUES (${address}, ${useBaseFee}, ${block.number})
      ON CONFLICT (contract_address) DO UPDATE
        SET fee_total = t.fee_total + ${useBaseFee}`;
  return undefined;
};

const addFeeToContract = async (
  block: BlockLondon,
  address: string,
  useBaseFee: number,
): Promise<void> =>
  Promise.all([
    addFeeToContractForTimeframe(block, "1h", address, useBaseFee),
    addFeeToContractForTimeframe(block, "24h", address, useBaseFee),
    addFeeToContractForTimeframe(block, "7d", address, useBaseFee),
    addFeeToContractForTimeframe(block, "30d", address, useBaseFee),
    addFeeToContractForTimeframe(block, "all", address, useBaseFee),
  ]).then(() => undefined);

export const updateTotalsWithFees = async (
  block: BlockLondon,
  baseFees: FeeBreakdown,
) => {
  const unknownDappFees = baseFees;

  await Promise.all(
    Object.entries(unknownDappFees.contract_use_fees).map(([address, fee]) =>
      addFeeToContract(block, address, fee!),
    ),
  );
  await ensureFreshTotals(Object.keys(unknownDappFees));
};

const timeframeHoursMap: Record<LimitedTimeframe, number> = {
  "1h": 1,
  "24h": 24,
  "7d": 24 * 7,
  "30d": 24 * 30,
};

const subtractStaleBaseFees = async (
  timeframe: LimitedTimeframe,
  oldestIncludedBlock: number,
  address: string,
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
    return;
  }

  // New oldest is the last stale one plus one.
  const oldestFreshBlockNumber = staleBlocks[staleBlocks.length - 1].number + 1;
  const staleSum = pipe(
    staleBlocks,
    A.map((block) => block.baseFees.contract_use_fees),
    BaseFees.sumFeeMaps,
    (maps) => maps[address] || 0,
  );

  await sql`
    UPDATE ${sql(table)}
    SET
      fee_total = fee_total - ${staleSum},
      oldest_included_block = ${oldestFreshBlockNumber}
    WHERE contract_address = ${address}`;
};

const ensureFreshTotal = async (
  timeframe: LimitedTimeframe,
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
      feeTotal: string;
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
        fees: Number(feeTotal),
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
        first.fees === second.fees ? 0 : first.fees > second.fees ? -1 : 1,
      equals: (first, second) => first.fees === second.fees,
    }),
    A.takeLeft(12),
  );
};

export type Leaderboard = {
  leaderboard1h: BaseFeeBurner[];
  leaderboard24h: BaseFeeBurner[];
  leaderboard7d: BaseFeeBurner[];
  leaderboard30d: BaseFeeBurner[];
  leaderboardAll: BaseFeeBurner[];
};

export const getNewLeaderboard = async (): Promise<Leaderboard> => {
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

  return {
    leaderboard1h,
    leaderboard24h,
    leaderboard7d,
    leaderboard30d,
    leaderboardAll,
  };
};

export const notifyNewLeaderboard = async (
  block: BlockLondon,
): Promise<void> => {
  await sql.notify(
    "burn-leaderboard-update",
    JSON.stringify({
      number: block.number,
    }),
  );
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
    await sql`
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

  await EthNode.webSocketOpen;

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
      Log.debug("all totals up to date, waiting 2s to check for new block");
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
    const block = await Blocks.getBlockWithRetry(nextBlockNumberToAnalyze);
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
