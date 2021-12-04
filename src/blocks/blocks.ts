import * as Sentry from "@sentry/node";
import { millisFromSeconds } from "../duration.js";
import { BlockLondon } from "../eth_node.js";
import { A, O, pipe, T, TAlt } from "../fp.js";
import { segmentTxrs, TxRWeb3London } from "../transactions.js";
import * as PerformanceMetrics from "../performance_metrics.js";
import * as EthNode from "../eth_node.js";
import { debug, warn } from "../log.js";
import { delay } from "../delay.js";
import {
  calcBlockBaseFeeSum,
  calcBlockFeeBreakdown,
  calcBlockTips,
  FeeBreakdown,
} from "../base_fees.js";
import { sql } from "../db.js";
import { usdToScaled } from "../scaling.js";
import { fromUnixTime } from "date-fns";
import { setContractsMinedAt, storeContracts } from "../contracts.js";
import { Granularity, granularitySqlMap } from "../burn-records/all.js";

export const londonHardForkBlockNumber = 12965000;

export type NewBlockPayload = {
  number: number;
};

export type BlockRow = {
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

const getBlockRow = (
  block: BlockDb,
  feeBreakdown: FeeBreakdown,
  tips: number,
  ethPrice: number,
): BlockRow => ({
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

export const getBlockHashIsKnown = (hash: string): T.Task<boolean> =>
  pipe(
    () => sql<{ isKnown: boolean }[]>`
      SELECT EXISTS(SELECT hash FROM blocks WHERE hash = ${hash}) AS is_known
    `,
    T.map((rows) => rows[0]?.isKnown === true ?? false),
  );

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

    warn(
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
    minedAt: fromUnixTime(block.timestamp),
    number: block.number,
    tips,
  };
};

export const storeBlock = (
  block: BlockLondon,
  txrs: TxRWeb3London[],
  ethPrice: number,
): T.Task<void> => {
  const blockDb = blockDbFromBlock(block, txrs, ethPrice);
  const feeBreakdown = calcBlockFeeBreakdown(block, txrs);
  const tips = calcBlockTips(block, txrs);
  const contractBaseFeesRows = getContractRows(block, feeBreakdown);
  const blockRow = getBlockRow(blockDb, feeBreakdown, tips, ethPrice);

  const addresses = contractBaseFeesRows.map(
    (contractBurnRow) => contractBurnRow.contract_address,
  );

  const storeBlockTask = () => sql`INSERT INTO blocks ${sql(blockRow)}`;

  const storeContractsTask = storeContracts(addresses);

  const storeContractsBaseFeesTask =
    contractBaseFeesRows.length !== 0
      ? () => sql`INSERT INTO contract_base_fees ${sql(contractBaseFeesRows)}`
      : T.of(undefined);

  const updateContractsMinedAtTask = pipe(
    getNewContractsFromBlock(txrs),
    (addresses) =>
      setContractsMinedAt(
        addresses,
        block.number,
        fromUnixTime(block.timestamp),
      ),
  );

  return pipe(
    getBlockHashIsKnown(block.parentHash),
    T.chainIOK((isParentHashKnown) => () => {
      if (!isParentHashKnown) {
        alert("store block, missed a block, stopping");
        throw new Error("missing block");
      }

      return undefined;
    }),
    T.chain(() =>
      TAlt.seqTSeqT(
        TAlt.seqTParT(storeContractsTask, storeBlockTask),
        TAlt.seqTParT(storeContractsBaseFeesTask, updateContractsMinedAtTask),
      ),
    ),
    T.map(() => undefined),
  );
};

export const updateBlock = (
  block: BlockLondon,
  txrs: TxRWeb3London[],
  ethPrice: number,
): T.Task<void> => {
  const blockDb = blockDbFromBlock(block, txrs, ethPrice);
  const feeBreakdown = calcBlockFeeBreakdown(block, txrs);
  const tips = calcBlockTips(block, txrs);
  const blockRow = getBlockRow(blockDb, feeBreakdown, tips, ethPrice);
  const contractBaseFeesRows = getContractRows(block, feeBreakdown);

  debug(
    `update number: ${block.number}, hash: ${block.hash}, parentHash: ${block.parentHash}`,
  );

  const addresses = contractBaseFeesRows.map(
    (contractBurnRow) => contractBurnRow.contract_address,
  );

  const updateBlockTask = pipe(
    () => sql`
      UPDATE blocks
      SET
        ${sql(blockRow)}
      WHERE
        number = ${block.number}
    `,
    T.map(() => undefined),
  );

  const updateContractBaseFeesTask = TAlt.seqTSeqT(
    () =>
      sql`DELETE FROM contract_base_fees WHERE block_number = ${block.number}`,
    contractBaseFeesRows.length !== 0
      ? () => sql`INSERT INTO contract_base_fees ${sql(contractBaseFeesRows)}`
      : T.of(undefined),
  );

  const storeContractsTask = storeContracts(addresses);

  const updateContractsMinedAtTask = pipe(
    getNewContractsFromBlock(txrs),
    (addresses) =>
      setContractsMinedAt(
        addresses,
        block.number,
        fromUnixTime(block.timestamp),
      ),
  );

  return pipe(
    getBlockHashIsKnown(block.parentHash),
    T.chainIOK((isParentHashKnown) => () => {
      if (!isParentHashKnown) {
        alert("update block, missed a block, stopping");
        throw new Error("missing block");
      }

      return undefined;
    }),
    T.chain(() =>
      TAlt.seqTSeqT(
        TAlt.seqTParT(storeContractsTask, updateBlockTask),
        TAlt.seqTParT(updateContractBaseFeesTask, updateContractsMinedAtTask),
      ),
    ),
    T.map(() => undefined),
  );
};

export const getIsKnownBlock = (blockNumber: number): T.Task<boolean> =>
  pipe(
    () =>
      sql<
        { isKnown: boolean }[]
      >`SELECT EXISTS(SELECT number FROM blocks WHERE number = ${blockNumber}) AS is_known`,
    T.map((rows) => rows[0]?.isKnown ?? false),
  );

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
  ethPriceCents: usdToScaled(row.ethPrice),
  ethTransferSum: row.ethTransferSum,
  gasUsed: row.gasUsed,
  hash: row.hash,
  minedAt: row.minedAt,
  number: row.number,
  tips: row.tips,
});

export const getBlocks = (
  from: number,
  upToIncluding: number,
): T.Task<BlockDb[]> =>
  pipe(
    () => sql<BlockDbRow[]>`
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
      WHERE number >= ${from}
      AND number <= ${upToIncluding}
      ORDER BY number ASC
    `,
    T.map(A.map(blockDbFromRow)),
  );

const getPastBlockNumber = async (
  referenceBlock: number,
  period: Granularity,
): Promise<number> => {
  const [{ minedAt }] = await sql<
    { minedAt: Date }[]
  >`SELECT mined_at FROM blocks WHERE number = ${referenceBlock}`;

  const interval = granularitySqlMap[period];

  const [block] = await sql<{ number: number }[]>`
    SELECT number FROM blocks
    ORDER BY ABS(EXTRACT(epoch FROM (${minedAt} - ${interval}::interval )))
    LIMIT 1
  `;

  return block.number;
};

export const getBlocksForGranularity = async (
  granularity: Granularity,
  referenceBlock: number,
): Promise<FeeBlockRow[]> => {
  const pastBlockNumber = await getPastBlockNumber(referenceBlock, granularity);
  return getFeeBlocks(pastBlockNumber, referenceBlock);
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
    (eth_price * 100)::bigint AS eth_price_cents
    gas_used,
    mined_at,
    number
  FROM blocks
  WHERE number >= ${from}
  AND number <= ${upToIncluding}
`;

export const getBlockRange = (from: number, toAndIncluding: number): number[] =>
  new Array(toAndIncluding - from + 1)
    .fill(undefined)
    .map((_, i) => toAndIncluding - i)
    .reverse();

export const getLatestKnownBlockNumber = async (): Promise<number> => {
  const rows = await sql<{ number: number }[]>`
    SELECT MAX(number) AS number FROM blocks
  `;

  return rows[0].number;
};
