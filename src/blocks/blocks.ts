import * as DateFns from "date-fns";
import {
  calcBlockBaseFeeSum,
  calcBlockFeeBreakdown,
  calcBlockTips,
  FeeBreakdown,
} from "../base_fees.js";
import * as Contracts from "../contracts/contracts.js";
import { sql, sqlT, sqlTVoid } from "../db.js";
import { delay } from "../delay.js";
import { millisFromSeconds } from "../duration.js";
import * as EthNode from "../eth_node.js";
import { BlockLondon } from "../eth_node.js";
import { A, NEA, O, pipe, T, TAlt, TO } from "../fp.js";
import * as Log from "../log.js";
import * as PerformanceMetrics from "../performance_metrics.js";
import * as TimeFrames from "../time_frames.js";
import { TimeFrame } from "../time_frames.js";
import { segmentTxrs, TxRWeb3London } from "../transactions.js";
import { usdToScaled } from "../usd_scaling.js";

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
  // TODO: rename this to timestamp.
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

const getNewContractsFromBlock = (txrs: TxRWeb3London[]) =>
  pipe(
    txrs,
    segmentTxrs,
    (segments) => segments.contractCreationTxrs,
    A.map((txr) => txr.contractAddress),
    A.map(O.fromNullable),
    A.compact,
    NEA.fromArray,
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
      Log.alert(`stuck fetching block, for more than ${tries * delaySeconds}s`);
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

const storeContractsBaseFeesTask = (
  block: BlockLondon,
  feeBreakdown: FeeBreakdown,
) =>
  pipe(
    Array.from(feeBreakdown.contract_use_fees.entries()),
    NEA.fromArray,
    O.map(
      A.map(
        ([address, baseFees]): ContractBaseFeesRow => ({
          base_fees: baseFees,
          block_number: block.number,
          contract_address: address,
        }),
      ),
    ),
    O.match(
      TAlt.constVoid,
      (insertables) =>
        sqlTVoid`
            INSERT INTO contract_base_fees ${sql(insertables)}
          `,
    ),
  );

export const storeBlock = async (
  block: BlockLondon,
  txrs: TxRWeb3London[],
  ethPrice: number,
): Promise<void> => {
  const blockDb = blockDbFromBlock(block, txrs, ethPrice);
  const feeBreakdown = calcBlockFeeBreakdown(block, txrs);
  const tips = calcBlockTips(block, txrs);
  const blockRow = insertableFromBlock(blockDb, feeBreakdown, tips, ethPrice);

  Log.debug(`storing block: ${block.number}, ${block.hash}`);
  const storeBlockTask = sqlT`INSERT INTO blocks ${sql(blockRow)}`;

  const updateContractsMinedAtTask = pipe(
    getNewContractsFromBlock(txrs),
    TO.fromOption,
    TO.chainTaskK((addresses) =>
      Contracts.setContractsMinedAt(
        addresses,
        block.number,
        DateFns.fromUnixTime(block.timestamp),
      ),
    ),
    TO.getOrElse(TAlt.constVoid),
  );

  const storeContractsTask = pipe(
    feeBreakdown.contract_use_fees,
    (map) => map.keys(),
    Array.from,
    NEA.fromArray,
    O.match(
      () => T.of(undefined),
      (addresses) => Contracts.storeContracts(addresses),
    ),
  );

  const isParentKnown = await getBlockHashIsKnown(block.parentHash);

  // Right before we store a block we check it is not breaking the logical chain. Every block should have a known parent in our DB. We have a check earlier on to store any missing parents that should take care of this. Remove this condition if it reliably does.
  if (!isParentKnown) {
    Log.alert("tried to store a block with no known parent");
    throw new Error("tried to store a block with no known parent");
  }

  await TAlt.seqTSeqT(
    TAlt.seqTParT(storeContractsTask, storeBlockTask),
    TAlt.seqTParT(
      storeContractsBaseFeesTask(block, feeBreakdown),
      updateContractsMinedAtTask,
    ),
  )();
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

export const getSyncedBlockHeight = async (): Promise<number> => {
  const rows = await sql<{ max: number }[]>`
    SELECT MAX(number) FROM blocks
  `;

  return rows[0].max;
};

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
  baseFeePerGas: string;
  contractCreationSum: number;
  ethPrice: number;
  ethPriceCents: string;
  ethTransferSum: number;
  gasUsed: string;
  hash: string;
  minedAt: Date;
  number: number;
  tips: number;
};

const blockDbFromRow = (row: BlockDbRow): BlockDb => ({
  baseFeePerGas: BigInt(row.baseFeePerGas),
  baseFeeSum: BigInt(row.baseFeePerGas) * BigInt(row.gasUsed),
  contractCreationSum: row.contractCreationSum,
  ethPrice: row.ethPrice,
  // TODO: should be scaled going in, read the scaled value.
  ethPriceCents: BigInt(row.ethPriceCents),
  ethTransferSum: row.ethTransferSum,
  gasUsed: BigInt(row.gasUsed),
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

// These blocks are minimized to only carry the information needed to calculate a record.
export type FeeBlockRow = {
  baseFeePerGas: string;
  ethPrice: number;
  // TODO: should be scaled going in, read the scaled value.
  ethPriceCents: string;
  gasUsed: string;
  minedAt: Date;
  number: number;
};

export type FeeBlockDb = {
  baseFeePerGas: bigint;
  ethPrice: number;
  // TODO: should be scaled going in, read the scaled value.
  ethPriceCents: bigint;
  gasUsed: bigint;
  minedAt: Date;
  number: number;
};

const feeBlockDbFromRow = (row: FeeBlockRow): FeeBlockDb => ({
  baseFeePerGas: BigInt(row.baseFeePerGas),
  ethPrice: row.ethPrice,
  ethPriceCents: BigInt(row.ethPriceCents),
  gasUsed: BigInt(row.gasUsed),
  minedAt: row.minedAt,
  number: row.number,
});

export const getFeeBlocks = (from: number, upToIncluding: number) =>
  pipe(
    sqlT<FeeBlockRow[]>`
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
    `,
    T.map(A.map(feeBlockDbFromRow)),
  );

export const getBlockRange = (from: number, toAndIncluding: number): number[] =>
  new Array(toAndIncluding - from + 1)
    .fill(undefined)
    .map((_, i) => toAndIncluding - i)
    .reverse();

export const getLastStoredBlock = async (): Promise<BlockDb> => {
  const rows = await sql<{ max: number }[]>`
    SELECT MAX(number) FROM blocks
  `;

  if (rows.length === 0) {
    throw new Error("can't get last stored block from empty table");
  }

  const [block] = await getBlocks(rows[0].max, rows[0].max);

  return block;
};

export const getIsBlockWithinTimeFrame = async (
  blockNumber: number,
  timeFrame: TimeFrame,
) => {
  if (timeFrame === "all") {
    return true;
  }

  const interval = TimeFrames.intervalSqlMap[timeFrame];

  const [exists] = await sql<{ exists: boolean }[]>`
    SELECT (
      SELECT mined_at FROM blocks WHERE number = ${blockNumber}
    ) >= (
      (SELECT MAX(mined_at) FROM blocks) - ${interval}::INTERVAL
    ) AS "exists"
  `;

  return exists;
};

export const getEarliestBlockInTimeFrame = (timeFrame: TimeFrame) =>
  timeFrame === "all"
    ? T.of(londonHardForkBlockNumber)
    : pipe(
        TimeFrames.intervalSqlMap[timeFrame],
        (interval) => sqlT<{ min: number }[]>`
          SELECT MIN(number) FROM blocks
          WHERE mined_at >= NOW() - ${interval}::interval
        `,
        T.map((rows) => rows[0].min),
      );
