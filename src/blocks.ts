import * as Sentry from "@sentry/node";
import * as EthNode from "./eth_node.js";
import { BlockLondon } from "./eth_node.js";
import * as Duration from "./duration.js";
import * as Log from "./log.js";
import { delay } from "./delay.js";
import * as PerformanceMetrics from "./performance_metrics.js";
import { sql } from "./db.js";
import { FeeBreakdown } from "./base_fees.js";
import { fromUnixTime } from "date-fns";
import * as BaseFees from "./base_fees.js";
import { hexToNumber } from "./hexadecimal.js";
import { pipe } from "fp-ts/lib/function.js";
import * as A from "fp-ts/lib/Array.js";
import * as T from "fp-ts/lib/Task.js";
import { seqSPar, seqTPar, seqTSeq } from "./sequence.js";
import { TxRWeb3London } from "./transactions.js";
import * as Contracts from "./contracts.js";
import * as Transactions from "./transactions.js";
import Config from "./config.js";
import * as DisplayProgress from "./display_progress.js";
import * as B from "fp-ts/lib/boolean.js";
import * as BurnRates from "./burn_rates.js";
import * as FeesBurned from "./fees_burned.js";
import * as Leaderboards from "./leaderboards.js";
import * as DerivedBlockStats from "./derived_block_stats.js";

type SyncStatus = "unknown" | "in-sync" | "out-of-sync";
let syncStatus: SyncStatus = "unknown";

const getSyncStatus = (): SyncStatus => syncStatus;
export const setSyncStatus = (newSyncStatus: SyncStatus): void => {
  syncStatus = newSyncStatus;
};

export const londonHardForkBlockNumber = 12965000;

export const getBlockRange = (from: number, toAndIncluding: number): number[] =>
  new Array(toAndIncluding - from + 1)
    .fill(undefined)
    .map((_, i) => toAndIncluding - i)
    .reverse();

export const getBlockWithRetry = async (
  blockNumber: number | "latest" | string,
): Promise<BlockLondon> => {
  const delayMilis = Duration.milisFromSeconds(3);
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

export const getLatestAnalyzedBlockNumber = (): Promise<number | undefined> =>
  sql`
    SELECT MAX(number) AS number FROM blocks
  `.then((result) => result[0]?.number || undefined);

type BlockRow = {
  hash: string;
  number: number;
  base_fees: unknown;
  mined_at: Date;
  tips: number;
  base_fee_sum: number;
  contract_creation_sum: number;
  eth_transfer_sum: number;
  base_fee_per_gas: number;
  gas_used: number;
};

const getBlockRow = (
  block: BlockLondon,
  feeBreakdown: FeeBreakdown,
  tips: number,
): BlockRow => ({
  hash: block.hash,
  number: block.number,
  base_fees: sql.json(feeBreakdown),
  mined_at: fromUnixTime(block.timestamp),
  tips: tips,
  base_fee_sum: BaseFees.calcBlockBaseFeeSum(block),
  contract_creation_sum: feeBreakdown.contract_creation_fees,
  eth_transfer_sum: feeBreakdown.transfers,
  base_fee_per_gas: hexToNumber(block.baseFeePerGas),
  gas_used: block.gasUsed,
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
    feeBreakdown.contract_use_fees,
    Object.entries,
    A.map(([address, baseFees]) => ({
      base_fees: baseFees,
      block_number: block.number,
      contract_address: address,
    })),
  );

const updateBlock = (
  block: BlockLondon,
  txrs: TxRWeb3London[],
): T.Task<void> => {
  const feeBreakdown = BaseFees.calcBlockFeeBreakdown(block, txrs);
  const tips = BaseFees.calcBlockTips(block, txrs);
  const blockRow = getBlockRow(block, feeBreakdown, tips);
  const contractBaseFeesRows = getContractRows(block, feeBreakdown);

  const updateBlockTask = () =>
    sql`
      UPDATE blocks
      SET
        ${sql(blockRow)}
      WHERE
        number = ${block.number}
    `.then(() => undefined);

  const updateContractBaseFeesTask = () =>
    sql.begin(async (sql) => {
      await sql`DELETE FROM contract_base_fees WHERE block_number = ${block.number}`;
      if (txrs.length !== 0) {
        await sql`INSERT INTO contract_base_fees ${sql(contractBaseFeesRows)}`;
      }
    });

  return pipe(
    T.sequenceArray([updateBlockTask, updateContractBaseFeesTask]),
    T.map(() => undefined),
  );
};

const storeBlock = (
  block: BlockLondon,
  txrs: TxRWeb3London[],
): T.Task<void> => {
  const feeBreakdown = BaseFees.calcBlockFeeBreakdown(block, txrs);
  const tips = BaseFees.calcBlockTips(block, txrs);
  const blockRow = getBlockRow(block, feeBreakdown, tips);
  const contractBaseFeesRows = getContractRows(block, feeBreakdown);

  const addresses = contractBaseFeesRows.map(
    (contractBurnRow) => contractBurnRow.contract_address,
  );

  const writeBlockT = () => sql`INSERT INTO blocks ${sql(blockRow)}`;

  if (contractBaseFeesRows.length === 0) {
    return pipe(
      seqTPar(Contracts.storeContracts(addresses), writeBlockT),
      T.map(() => undefined),
    );
  }

  const writeContractBaseFeesT = () =>
    sql`INSERT INTO contract_base_fees ${sql(contractBaseFeesRows)}`;

  return pipe(
    seqTSeq(
      seqTPar(Contracts.storeContracts(addresses), writeBlockT),
      writeContractBaseFeesT,
    ),
    T.map(() => undefined),
  );
};

export const getIsKnownBlock = (block: BlockLondon): T.Task<boolean> =>
  pipe(
    () =>
      sql<
        { isKnown: boolean }[]
      >`SELECT EXISTS(SELECT number FROM blocks WHERE number = ${block.number}) AS is_known`,
    T.map((rows) => rows[0]?.isKnown ?? false),
  );

export type NewBlockPayload = {
  number: number;
};

const notifyNewBlock = (block: BlockLondon): T.Task<void> => {
  const payload: NewBlockPayload = {
    number: block.number,
  };

  return pipe(
    () => sql.notify("new-block", JSON.stringify(payload)),
    T.map(() => undefined),
  );
};

const updateDerivedBlockStats = (block: BlockLondon) => {
  const feesBurned = FeesBurned.calcFeesBurned(block);
  const burnRates = BurnRates.calcBurnRates(block);
  const leaderboards = Leaderboards.calcLeaderboards(block);

  return pipe(
    seqSPar({ burnRates, feesBurned, leaderboards }),
    T.chain((derivedBlockStats) =>
      DerivedBlockStats.storeDerivedBlockStats(block, derivedBlockStats),
    ),
  );
};

export const storeNewBlock = (
  knownBlocks: Set<number>,
  blockNumber: number,
  notify: boolean,
): T.Task<void> =>
  pipe(
    () => getBlockWithRetry(blockNumber),
    T.apFirst(() => {
      Log.debug(`analyzing block ${blockNumber}`);
      return Promise.resolve();
    }),
    T.chain((block) =>
      seqTPar(T.of(block), () => Transactions.getTxrsWithRetry(block)),
    ),
    T.chainFirstIOK(() => () => {
      if (Config.showProgress) {
        DisplayProgress.onBlockAnalyzed();
      }
    }),
    T.chain(([block, txrs]) =>
      pipe(
        knownBlocks.has(block.number),
        B.match(
          () => storeBlock(block, txrs),
          () => updateBlock(block, txrs),
        ),
        T.chain(() =>
          pipe(
            getSyncStatus() === "in-sync",
            B.match(
              () => T.of(undefined),
              () =>
                pipe(
                  updateDerivedBlockStats(block),
                  T.chain(() =>
                    notify ? notifyNewBlock(block) : T.of(undefined),
                  ),
                ),
            ),
          ),
        ),
      ),
    ),
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
