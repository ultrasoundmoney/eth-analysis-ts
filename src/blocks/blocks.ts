import * as Sentry from "@sentry/node";
import { millisFromSeconds } from "../duration.js";
import { BlockLondon } from "../eth_node.js";
import { A, O, pipe, T, TAlt } from "../fp.js";
import * as DateFns from "date-fns";
import { segmentTxrs, TxRWeb3London } from "../transactions.js";
import * as PerformanceMetrics from "../performance_metrics.js";
import * as EthNode from "../eth_node.js";
import * as Log from "../log.js";
import { delay } from "../delay.js";
import {
  calcBlockBaseFeeSum,
  calcBlockFeeBreakdown,
  calcBlockTips,
  FeeBreakdown,
} from "../base_fees.js";
import { sql, SqlArg } from "../db.js";
import { usdToScaled } from "../scaling.js";
import { setContractsMinedAt, storeContracts } from "../contracts.js";
import { granularitySqlMap } from "../burn-records/all.js";
import { Granularity } from "../burn-records/burn_records.js";

export const londonHardForkBlockNumber = 12965000;

export type NewBlockPayload = {
  number: number;
};

export type BlockDbInsertable = {
  hash: string;
  number: number;
  mined_at: Date;
  tips: number;
  base_fee_sum: number;
  base_fee_sum_256: string;
  contract_creation_sum: number;
  eth_transfer_sum: number;
  base_fee_per_gas: bigint;
  gas_used: bigint;
  eth_price?: number;
};

export type BlockDb = {
  baseFeePerGas: bigint;
  baseFeeSum: bigint;
  contractCreationSum: number;
  ethPrice: number;
  ethPriceCents: bigint;
  ethTransferSum: number;
  gasUsed: bigint;
  hash: string;
  minedAt: Date;
  number: number;
  tips: number;
};

const insertableFromBlock = (
  block: BlockDb,
  feeBreakdown: FeeBreakdown,
  tips: number,
  ethPrice: number,
): BlockDbInsertable => ({
  ...(typeof ethPrice === "number" ? { eth_price: ethPrice } : undefined),
  base_fee_per_gas: block.baseFeePerGas,
  base_fee_sum: Number(block.baseFeeSum),
  base_fee_sum_256: block.baseFeeSum.toString(),
  contract_creation_sum: feeBreakdown.contract_creation_fees,
  eth_transfer_sum: feeBreakdown.transfers,
  gas_used: block.gasUsed,
  hash: block.hash,
  mined_at: block.minedAt,
  number: block.number,
  tips: tips,
});

type ContractBaseFeesRow = {
  contract_address: string;
  base_fees: number;
  block_number: number;
};

const getContractRows = (
  block: BlockLondon,
  feeBreakdown: FeeBreakdown,
): ContractBaseFeesRow[] =>
  pipe(
    Array.from(feeBreakdown.contract_use_fees.entries()),
    A.map(([address, baseFees]) => ({
      base_fees: baseFees,
      block_number: block.number,
      contract_address: address,
    })),
  );

const getNewContractsFromBlock = (txrs: TxRWeb3London[]): string[] =>
  pipe(
    txrs,
    segmentTxrs,
    (segments) => segments.contractCreationTxrs,
    A.map((txr) => txr.contractAddress),
    A.map(O.fromNullable),
    A.compact,
  );

export const getBlockHashIsKnown = async (hash: string): Promise<boolean> => {
  const [block] = await sql<{ isKnown: boolean }[]>`
      SELECT EXISTS(SELECT hash FROM blocks WHERE hash = ${hash}) AS is_known
    `;

  return block?.isKnown ?? false;
};

export const getBlockWithRetry = async (
  blockNumber: number | "latest" | string,
): Promise<BlockLondon> => {
  const delayMilis = millisFromSeconds(3);
  const delaySeconds = delayMilis * 1000;
  let tries = 0;

  // Retry continuously
  // eslint-disable-next-line no-constant-condition
  while (true) {
    tries += tries + 1;

    const maybeBlock = await EthNode.getBlock(blockNumber);

    if (typeof maybeBlock?.hash === "string") {
      PerformanceMetrics.onBlockReceived();
      return maybeBlock;
    }

    if (tries === 10) {
      Sentry.captureException(
        new Error(
          `stuck fetching block, for more than ${tries * delaySeconds}s`,
        ),
      );
    }

    if (tries > 20) {
      throw new Error("failed to fetch block, stayed null");
    }

    Log.warn(
      `asked for block ${blockNumber}, got null, waiting ${delaySeconds}s and trying again`,
    );
    await delay(delayMilis);
  }
};

export const blockDbFromBlock = (
  block: BlockLondon,
  txrs: TxRWeb3London[],
  ethPrice: number,
): BlockDb => {
  const feeBreakdown = calcBlockFeeBreakdown(block, txrs);
  const tips = calcBlockTips(block, txrs);

  return {
    baseFeePerGas: BigInt(block.baseFeePerGas),
    baseFeeSum: calcBlockBaseFeeSum(block),
    contractCreationSum: feeBreakdown.contract_creation_fees,
    ethPrice,
    ethPriceCents: usdToScaled(ethPrice),
    ethTransferSum: feeBreakdown.transfers,
    gasUsed: BigInt(block.gasUsed),
    hash: block.hash,
    minedAt: DateFns.fromUnixTime(block.timestamp),
    number: block.number,
    tips,
  };
};

export const storeBlock = async (
  block: BlockLondon,
  txrs: TxRWeb3London[],
  ethPrice: number,
): Promise<void> => {
  const blockDb = blockDbFromBlock(block, txrs, ethPrice);
  const feeBreakdown = calcBlockFeeBreakdown(block, txrs);
  const tips = calcBlockTips(block, txrs);
  const contractBaseFeesRows = getContractRows(block, feeBreakdown);
  const blockRow = insertableFromBlock(blockDb, feeBreakdown, tips, ethPrice);

  const addresses = contractBaseFeesRows.map(
    (contractBurnRow) => contractBurnRow.contract_address,
  );

  Log.debug(`storing block: ${block.number}, ${block.hash}`);
  const storeBlockTask = () => sql`INSERT INTO blocks ${sql(blockRow)}`;

  const storeContractsBaseFeesTask =
    contractBaseFeesRows.length !== 0
      ? async () =>
          sql`INSERT INTO contract_base_fees ${sql(contractBaseFeesRows)}`
      : () => undefined;

  const updateContractsMinedAtTask = async () => {
    const addresses = getNewContractsFromBlock(txrs);
    return setContractsMinedAt(
      addresses,
      block.number,
      DateFns.fromUnixTime(block.timestamp),
    );
  };

  const isParentKnown = await getBlockHashIsKnown(block.parentHash);

  if (!isParentKnown) {
    // TODO: should never happen anymore, remove this if no alert shows up.
    // We're missing the parent hash, update the previous block.
    Log.alert("sync block, parent hash not found, storing parent again");
    throw new Error("tried to store a block out of order");
  }

  await Promise.all([storeContracts(addresses), storeBlockTask()]);
  await Promise.all([
    storeContractsBaseFeesTask(),
    updateContractsMinedAtTask(),
  ]);
};

export const deleteDerivedBlockStats = async (
  blockNumber: number,
): Promise<void> => {
  await sql`
    DELETE FROM derived_block_stats
    WHERE block_number = ${blockNumber}
  `;
};

export const deleteBlock = async (blockNumber: number): Promise<void> => {
  await sql`
    DELETE FROM blocks
    WHERE number = ${blockNumber}
  `;
};

export const deleteContractBaseFees = async (
  blockNumber: number,
): Promise<void> => {
  await sql`
    DELETE FROM contract_base_fees
    WHERE block_number = ${blockNumber}
  `;
};

export const getIsKnownBlock = (blockNumber: number): T.Task<boolean> =>
  pipe(
    () =>
      sql<
        { isKnown: boolean }[]
      >`SELECT EXISTS(SELECT number FROM blocks WHERE number = ${blockNumber}) AS is_known`,
    T.map((rows) => rows[0]?.isKnown ?? false),
  );

export const getSyncedBlockHeight = async (): Promise<number> => {
  const rows = await sql<{ max: number }[]>`
    SELECT MAX(number) FROM blocks
  `;

  return rows[0].max;
};

export const getKnownBlocks = (): T.Task<Set<number>> =>
  pipe(
    () => sql<{ number: number }[]>`SELECT number FROM blocks`,
    T.map((rows) =>
      pipe(
        rows,
        A.map((row) => row.number),
        (numbers) => new Set(numbers),
      ),
    ),
  );

export const getLastNKnownBlocks = (count: number): T.Task<Set<number>> =>
  pipe(
    () => sql<{ number: number }[]>`
      SELECT number
      FROM blocks
      ORDER BY number DESC
      LIMIT ${count}
    `,
    T.map((rows) =>
      pipe(
        rows,
        A.map((row) => row.number),
        (numbers) => new Set(numbers),
      ),
    ),
  );

export const getBaseFeesPerGas = (
  blockNumber: number,
): T.Task<number | undefined> => {
  return pipe(
    () => sql<{ baseFeePerGas: number }[]>`
      SELECT base_fee_per_gas FROM blocks
      WHERE number = ${blockNumber}
    `,
    T.map((rows) =>
      typeof rows[0]?.baseFeePerGas === "bigint"
        ? Number(rows[0].baseFeePerGas)
        : undefined,
    ),
  );
};

export const setEthPrice = (
  blockNumber: number,
  ethPrice: number,
): T.Task<void> =>
  pipe(
    () => sql`
      UPDATE blocks
      SET eth_price = ${ethPrice}
      WHERE number = ${blockNumber}
    `,
    T.map(() => undefined),
  );

export const getLatestBaseFeePerGas = (): T.Task<number> =>
  pipe(
    () => sql<{ baseFeePerGas: number }[]>`
      SELECT base_fee_per_gas FROM blocks
      ORDER BY number DESC
      LIMIT 1
    `,
    T.map((rows) =>
      typeof rows[0]?.baseFeePerGas === "bigint"
        ? Number(rows[0].baseFeePerGas)
        : 0,
    ),
  );

type BlockDbRow = {
  baseFeePerGas: bigint;
  contractCreationSum: number;
  ethPrice: number;
  ethPriceCents: bigint;
  ethTransferSum: number;
  gasUsed: bigint;
  hash: string;
  minedAt: Date;
  number: number;
  tips: number;
};

const blockDbFromRow = (row: BlockDbRow): BlockDb => ({
  baseFeePerGas: row.baseFeePerGas,
  baseFeeSum: row.baseFeePerGas * row.gasUsed,
  contractCreationSum: row.contractCreationSum,
  ethPrice: row.ethPrice,
  // TODO: should be scaled going in, read the scaled value.
  ethPriceCents: row.ethPriceCents,
  ethTransferSum: row.ethTransferSum,
  gasUsed: row.gasUsed,
  hash: row.hash,
  minedAt: row.minedAt,
  number: row.number,
  tips: row.tips,
});

export const getBlocks = async (
  from: number,
  upToIncluding: number,
): Promise<BlockDb[]> => {
  const rows = await sql<BlockDbRow[]>`
    SELECT
      base_fee_per_gas,
      contract_creation_sum,
      eth_price,
      (eth_price * 100)::bigint AS eth_price_cents,
      eth_transfer_sum,
      gas_used,
      hash,
      mined_at,
      number,
      tips
    FROM blocks
    WHERE number >= ${from}
    AND number <= ${upToIncluding}
    ORDER BY number ASC
  `;

  return rows.map(blockDbFromRow);
};

export const getPastBlock = async (
  referenceBlock: BlockDb,
  interval: string,
): Promise<BlockDb> => {
  const [row] = await sql<BlockDbRow[]>`
    SELECT
      base_fee_per_gas,
      contract_creation_sum,
      eth_price,
      eth_transfer_sum,
      gas_used,
      hash,
      mined_at,
      number,
      tips
    FROM blocks
    ORDER BY ABS(EXTRACT(epoch FROM (${referenceBlock.minedAt} - ${interval}::interval )))
    LIMIT 1
  `;

  return blockDbFromRow(row);
};

export const getBlocksForGranularity = async (
  granularity: Granularity,
  referenceBlock: BlockDb,
): Promise<FeeBlockRow[]> => {
  const interval = granularitySqlMap[granularity];
  const pastBlock = await getPastBlock(referenceBlock, interval);
  return getFeeBlocks(pastBlock.number, referenceBlock.number);
};

// These blocks are minimized to only carry the information needed to calculate a record.
export type FeeBlockRow = {
  baseFeePerGas: bigint;
  ethPrice: number;
  // TODO: should be scaled going in, read the scaled value.
  ethPriceCents: bigint;
  gasUsed: bigint;
  minedAt: Date;
  number: number;
};

export const getFeeBlocks = async (
  from: number,
  upToIncluding: number,
): Promise<FeeBlockRow[]> => sql<FeeBlockRow[]>`
  SELECT
    base_fee_per_gas,
    eth_price,
    (eth_price * 100)::bigint AS eth_price_cents,
    gas_used,
    mined_at,
    number
  FROM blocks
  WHERE number >= ${from}
  AND number <= ${upToIncluding}
  ORDER BY number DESC
`;

export const getBlockRange = (from: number, toAndIncluding: number): number[] =>
  new Array(toAndIncluding - from + 1)
    .fill(undefined)
    .map((_, i) => toAndIncluding - i)
    .reverse();

export const getLastStoredBlock = async (): Promise<BlockDb> => {
  const rows = await sql<{ number: number }[]>`
    SELECT MAX(number) AS number FROM blocks
  `;

  if (rows.length === 0) {
    throw new Error("can't get last stored block from empty table");
  }

  const [block] = await getBlocks(rows[0].number, rows[0].number);

  return block;
};
