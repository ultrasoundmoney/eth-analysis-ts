import * as DateFns from "date-fns";
import { setTimeout } from "timers/promises";
import {
  calcBlockBaseFeeSum,
  calcBlockTips,
  FeeSegments,
  sumFeeSegments,
} from "../base_fees.js";
import * as Contracts from "../contracts/contracts.js";
import * as ContractBaseFees from "../contract_base_fees.js";
import * as Db from "../db.js";
import { sql, sqlT } from "../db.js";
import { millisFromSeconds } from "../duration.js";
import * as ExecutionNode from "../execution_node.js";
import { A, flow, NEA, O, Ord, pipe, T, TAlt, TO, TOAlt } from "../fp.js";
import * as Hexadecimal from "../hexadecimal.js";
import * as Log from "../log.js";
import * as PerformanceMetrics from "../performance_metrics.js";
import * as TimeFrames from "../time_frames.js";
import * as Transactions from "../transactions.js";

export const mergeBlockNumber = 15537393;
export const mergeBlockDate = new Date("2022-09-15T06:42:42Z");

export const londonHardForkBlockNumber = 12965000;
export const londonHardForkBlockDate = new Date("2021-08-05T12:33:42Z");

/**
 * This is a block as we get it from an eth node, after we drop fields we don't need and decode ones we use.
 */
export type BlockNodeV2 = {
  baseFeePerGas: number;
  baseFeePerGasBI: bigint;
  difficulty: bigint;
  gasLimit: number;
  gasLimitBI: bigint;
  gasUsed: number;
  gasUsedBI: bigint;
  hash: string;
  number: number;
  parentHash: string;
  size: number;
  timestamp: Date;
  transactions: string[];
};

export const blockV1FromNode = (
  blockNode: ExecutionNode.BlockNodeV1,
): BlockNodeV2 => ({
  baseFeePerGas: Number(blockNode.baseFeePerGas),
  baseFeePerGasBI: BigInt(blockNode.baseFeePerGas),
  difficulty: BigInt(blockNode.difficulty),
  gasLimit: Hexadecimal.numberFromHex(blockNode.gasLimit),
  gasLimitBI: BigInt(blockNode.gasLimit),
  gasUsed: Hexadecimal.numberFromHex(blockNode.gasUsed),
  gasUsedBI: BigInt(blockNode.gasUsed),
  hash: blockNode.hash,
  number: Hexadecimal.numberFromHex(blockNode.number),
  parentHash: blockNode.parentHash,
  size: Hexadecimal.numberFromHex(blockNode.size),
  timestamp: DateFns.fromUnixTime(
    Hexadecimal.numberFromHex(blockNode.timestamp),
  ),
  transactions: blockNode.transactions,
});

export type BlockDbInsertable = {
  base_fee_per_gas: string;
  base_fee_sum: number;
  base_fee_sum_256: string;
  contract_creation_sum: number;
  difficulty: string;
  eth_price?: number;
  eth_transfer_sum: number;
  gas_used: string;
  hash: string;
  mined_at: Date;
  number: number;
  tips: number;
};

export type BlockV1 = {
  baseFeePerGas: bigint;
  baseFeeSum: bigint;
  contractCreationSum: number;
  difficulty: bigint | undefined;
  ethPrice: number;
  ethTransferSum: number;
  gasUsed: bigint;
  hash: string;
  // TODO: rename this to timestamp.
  minedAt: Date;
  number: number;
  tips: number;
};

export const insertableFromBlock = (block: BlockV1): BlockDbInsertable => ({
  base_fee_per_gas: String(block.baseFeePerGas),
  base_fee_sum: Number(block.baseFeeSum),
  base_fee_sum_256: String(block.baseFeeSum),
  contract_creation_sum: block.contractCreationSum,
  difficulty: String(block.difficulty),
  eth_price: block.ethPrice,
  eth_transfer_sum: block.ethTransferSum,
  gas_used: String(block.gasUsed),
  hash: block.hash,
  mined_at: block.minedAt,
  number: block.number,
  tips: block.tips,
});

export type ContractBaseFeesInsertable = {
  base_fees: number;
  base_fees_256: string;
  block_number: number;
  contract_address: string;
  gas_used: string;
  transaction_count: number;
};

export const getBlockHashIsKnown = async (hash: string): Promise<boolean> => {
  const [block] = await sql<{ isKnown: boolean }[]>`
    SELECT EXISTS(SELECT hash FROM blocks WHERE hash = ${hash}) AS is_known
  `;

  return block?.isKnown ?? false;
};

export const getBlockWithRetry = async (
  blockNumber: number | "latest" | string,
): Promise<BlockNodeV2> => {
  const delayMilis = millisFromSeconds(3);
  const delaySeconds = delayMilis * 1000;
  let tries = 0;

  // Retry continuously
  // eslint-disable-next-line no-constant-condition
  while (true) {
    tries += tries + 1;

    if (tries > 20) {
      throw new Error("failed to fetch block, stayed null");
    }

    try {
        const maybeBlock = await ExecutionNode.getBlock(blockNumber);

        if (typeof maybeBlock?.hash === "string") {
        PerformanceMetrics.onBlockReceived();
        return blockV1FromNode(maybeBlock);
        }

        if (tries === 10) {
        Log.alert(`stuck fetching block, for more than ${tries * delaySeconds}s`);
        }

        Log.warn(
        `asked for block ${blockNumber}, got null, waiting ${delaySeconds}s and trying again`,
        );
        await setTimeout(delayMilis);
    } catch (e: any) {
        Log.warn(
        `asked for block ${blockNumber}, threw error ${e.toString()}`,
        );
        await setTimeout(delayMilis);
    }
  }
};

export const getBlockSafe = (
  blockNumber: number | "latest" | string,
): TO.TaskOption<BlockNodeV2> =>
  pipe(
    () => ExecutionNode.getBlock(blockNumber),
    T.map(O.fromNullable),
    TO.map(blockV1FromNode),
  );

export const getBlockByHash = (hash: string) =>
  pipe(
    () => ExecutionNode.getRawBlockByHash(hash),
    T.map(O.fromNullable),
    TO.map(blockV1FromNode),
  );

export const blockDbFromAnalysis = (
  block: BlockNodeV2,
  feeSegments: FeeSegments,
  tips: number,
  ethPrice: number,
): BlockV1 => ({
  baseFeePerGas: BigInt(block.baseFeePerGas),
  baseFeeSum: calcBlockBaseFeeSum(block),
  contractCreationSum: feeSegments.creationsSum,
  difficulty: block.difficulty,
  ethPrice,
  ethTransferSum: feeSegments.transfersSum,
  gasUsed: BigInt(block.gasUsed),
  hash: block.hash,
  minedAt: block.timestamp,
  number: block.number,
  tips,
});

export const countTransactionsPerContract = (
  transactionReceipts: Transactions.TransactionReceiptV1[],
) =>
  pipe(
    transactionReceipts,
    A.reduce(new Map<string, number>(), (map, txr) =>
      pipe(
        txr.to,
        O.match(
          // Contract creation, skip.
          () => map,
          (to) =>
            pipe(map.get(to) ?? 0, (currentCount) =>
              map.set(to, currentCount + 1),
            ),
        ),
      ),
    ),
  );

export const storeBlock = async (
  block: BlockNodeV2,
  transactionReceipts: Transactions.TransactionReceiptV1[],
  ethPrice: number,
): Promise<void> => {
  const transactionSegments =
    Transactions.segmentTransactions(transactionReceipts);
  const feeSegments = sumFeeSegments(block, transactionSegments, ethPrice);
  const tips = calcBlockTips(block, transactionReceipts);
  const blockDb = blockDbFromAnalysis(block, feeSegments, tips, ethPrice);
  const transactionCounts = countTransactionsPerContract(
    transactionSegments.other,
  );
  const blockInsertable = insertableFromBlock(blockDb);

  Log.debug(`storing block: ${block.number}, ${block.hash}`);
  const storeBlockTask = sqlT`
    INSERT INTO blocks ${sql(blockInsertable)}
  `;

  const updateContractsMinedAtTask = pipe(
    Transactions.getNewContracts(transactionReceipts),
    TO.fromOption,
    TO.chainTaskK((addresses) =>
      Contracts.setContractsMinedAt(addresses, block.number, block.timestamp),
    ),
  );

  const storeContractsTask = pipe(
    feeSegments.contractSumsEth,
    (map) => Array.from(map.keys()),
    NEA.fromArray,
    O.match(
      () => T.of(undefined),
      (addresses) => Contracts.storeContracts(addresses),
    ),
  );

  const isParentKnown = await getBlockHashIsKnown(block.parentHash);

  // Right before we store a block we check it is not breaking the logical chain. Every block should have a known parent in our DB. We have a check earlier on to store any missing parents that should take care of this. Remove this condition if it reliably does.
  if (!isParentKnown) {
    const lastStoredBlock = await getLastStoredBlock()();
    Log.alert(
      `tried to store a block with no known parent, last stored: ${lastStoredBlock.number} - ${lastStoredBlock.hash}, trying to store: ${block.number} - ${block.hash}`,
    );
    throw new Error("tried to store a block with no known parent");
  }

  await TAlt.seqTSeq(
    TAlt.seqTPar(storeContractsTask, storeBlockTask),
    TAlt.seqTPar(
      ContractBaseFees.storeContractBaseFees(
        block,
        feeSegments,
        transactionCounts,
      ),
      updateContractsMinedAtTask,
    ),
  )();
};

export const deleteBlock = (blockNumber: number) =>
  Db.sqlTVoid`
    DELETE FROM blocks
    WHERE number = ${blockNumber}
  `;

export const getSyncedBlockHeight = async (): Promise<number> => {
  const rows = await sql<{ max: number }[]>`
    SELECT MAX(number) FROM blocks
  `;

  return rows[0].max;
};

export const getLatestBaseFeePerGas = (): T.Task<number> =>
  pipe(
    sqlT<{ baseFeePerGas: number }[]>`
      SELECT base_fee_per_gas FROM blocks
      ORDER BY number DESC
      LIMIT 1
    `,
    T.map((rows) => Number(rows[0].baseFeePerGas)),
  );

type BlockDbRow = {
  baseFeePerGas: string;
  contractCreationSum: number;
  difficulty: string | null;
  ethPrice: number;
  ethTransferSum: number;
  gasUsed: string;
  hash: string;
  minedAt: Date;
  number: number;
  tips: number;
};

const blockDbFromRow = (row: BlockDbRow): BlockV1 => ({
  baseFeePerGas: BigInt(row.baseFeePerGas),
  baseFeeSum: BigInt(row.baseFeePerGas) * BigInt(row.gasUsed),
  contractCreationSum: row.contractCreationSum,
  difficulty: row.difficulty !== null ? BigInt(row.difficulty) : undefined,
  ethPrice: row.ethPrice,
  ethTransferSum: row.ethTransferSum,
  gasUsed: BigInt(row.gasUsed),
  hash: row.hash,
  minedAt: row.minedAt,
  number: row.number,
  tips: row.tips,
});

export const getBlocks = (
  from: number,
  upToIncluding: number,
): T.Task<BlockV1[]> =>
  pipe(
    sqlT<BlockDbRow[]>`
      SELECT
        base_fee_per_gas,
        contract_creation_sum,
        difficulty,
        eth_price,
        eth_transfer_sum,
        difficulty,
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

export const getBlock = (blockNumber: number) =>
  pipe(getBlocks(blockNumber, blockNumber), T.map(A.head));

export const getBlocksFromAndIncluding = (blockNumber: number) =>
  pipe(
    sqlT<BlockDbRow[]>`
      SELECT
        base_fee_per_gas,
        contract_creation_sum,
        difficulty,
        eth_price,
        eth_transfer_sum,
        gas_used,
        hash,
        mined_at,
        number,
        tips
      FROM blocks
      WHERE number >= ${blockNumber}
      ORDER BY number ASC
    `,
    T.map(A.map(blockDbFromRow)),
  );

export const getBlockRange = (from: number, toAndIncluding: number): number[] =>
  new Array(toAndIncluding - from + 1)
    .fill(undefined)
    .map((_, i) => toAndIncluding - i)
    .reverse();

export const getLastStoredBlock = () =>
  pipe(
    sqlT<{ max: number }[]>`
      SELECT MAX(number) FROM blocks
    `,
    T.chain(
      flow(
        (rows) => rows[0]?.max,
        O.fromNullable,
        TO.fromOption,
        TO.chainTaskK((max) => getBlocks(max, max)),
        TO.chainOptionK(flow((rows) => rows[0], O.fromNullable)),
        TOAlt.getOrThrow("can't get last stored block from empty table"),
      ),
    ),
  );

export const getEarliestBlockInTimeFrame = (
  timeFrame: TimeFrames.TimeFrameNext,
) =>
  timeFrame === "all"
    ? T.of(londonHardForkBlockNumber)
    : timeFrame === "since_merge"
    ? T.of(mergeBlockNumber)
    : pipe(
        TimeFrames.intervalSqlMapNext[timeFrame],
        (interval) => () =>
          sql<{ min: number }[]>`
            SELECT MIN(number) FROM blocks
            WHERE mined_at >= NOW() - ${interval}::interval
          `,
        T.map((rows) => rows[0].min),
      );

export const sortAsc = Ord.fromCompare<BlockV1>((a, b) =>
  a.number < b.number ? -1 : a.number > b.number ? 1 : 0,
);

export const sortDesc = Ord.fromCompare<BlockV1>((a, b) =>
  a.number < b.number ? 1 : a.number > b.number ? -1 : 0,
);

export const getPreviousBlock = (block: BlockV1) =>
  pipe(getBlocks(block.number - 1, block.number - 1), T.map(A.head));
