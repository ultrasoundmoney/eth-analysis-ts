import PQueue from "p-queue";
import { calcBlockFeeBreakdown } from "../base_fees.js";
import { calcBaseFeeSums } from "../base_fee_sums.js";
// import * as BurnRecords from "../burn-records/burn_records.js";
// import * as BurnRecordsNewHead from "../burn-records/new_head.js";
import { calcBurnRates } from "../burn_rates.js";
import * as Contracts from "../contracts.js";
import { sql } from "../db.js";
import * as DerivedBlockStats from "../derived_block_stats.js";
import { BlockLondon, Head } from "../eth_node.js";
import { getPriceForOldBlock } from "../eth_prices.js";
import { pipe, T, TAlt } from "../fp.js";
import * as Leaderboards from "../leaderboards.js";
import { LeaderboardEntries } from "../leaderboards.js";
import * as LeaderboardsAll from "../leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "../leaderboards_limited_timeframe.js";
import * as Log from "../log.js";
import { logPerf, logPerfT } from "../performance.js";
import { getTxrsWithRetry } from "../transactions.js";
import * as Blocks from "./blocks.js";
import { NewBlockPayload } from "./blocks.js";

export const newBlockQueue = new PQueue({
  concurrency: 1,
  autoStart: false,
});

const rollbackBlock = async (blockNumber: number): Promise<void> => {
  Log.info(`rolling back block: ${blockNumber}`);

  const t0 = performance.now();

  const sumsToRollback = await Leaderboards.getRangeBaseFees(
    blockNumber,
    blockNumber,
  )();
  LeaderboardsLimitedTimeframe.onRollback(blockNumber, sumsToRollback);
  await Promise.all([
    LeaderboardsAll.removeContractBaseFeeSums(sumsToRollback),
    LeaderboardsAll.setNewestIncludedBlockNumber(blockNumber - 1),
    // BurnRecordsNewHead.onRollback(blockNumber),
  ]);

  await Contracts.deleteContractsMinedAt(blockNumber);
  await Blocks.deleteContractBaseFees(blockNumber);
  await Blocks.deleteDerivedBlockStats(blockNumber);
  await Blocks.deleteBlock(blockNumber);

  logPerf("rollback", t0);
};

export const addBlock = async (head: Head): Promise<void> => {
  Log.debug(`add block from new head ${head.number}`);
  const block = await Blocks.getBlockWithRetry(head.number);

  if (head.hash !== block.hash) {
    Log.warn("queued head is no longer valid, skipping");
    return;
  }

  const isParentKnown = await Blocks.getBlockHashIsKnown(block.parentHash);

  if (!isParentKnown) {
    // TODO: should never happen anymore, remove this if no alert shows up.
    // We're missing the parent hash, update the previous block.
    Log.alert("got new head, parent hash not found, storing parent again");
    const previousBlock = await Blocks.getBlockWithRetry(head.number - 1);
    await addBlock(previousBlock);
  }

  const syncedBlockHeight = await Blocks.getSyncedBlockHeight();
  if (block.number <= syncedBlockHeight) {
    await rollbackBlock(block.number);
  }

  const [txrs, ethPrice] = await Promise.all([
    getTxrsWithRetry(block),
    getPriceForOldBlock(block),
  ]);
  await Blocks.storeBlock(block, txrs, ethPrice.ethusd);

  const feeBreakdown = calcBlockFeeBreakdown(block, txrs, ethPrice.ethusd);

  const blockDb = Blocks.blockDbFromBlock(block, txrs, ethPrice.ethusd);

  const tStartAnalyze = performance.now();

  LeaderboardsLimitedTimeframe.addBlockForAllTimeframes(
    blockDb,
    feeBreakdown.contract_use_fees,
    feeBreakdown.contract_use_fees_usd!,
  );

  const addToLeaderboardAllTask = () =>
    LeaderboardsAll.addBlock(
      block.number,
      feeBreakdown.contract_use_fees,
      feeBreakdown.contract_use_fees_usd!,
    );

  await Promise.all([
    LeaderboardsLimitedTimeframe.removeExpiredBlocksFromSumsForAllTimeframes(),
    addToLeaderboardAllTask(),
    // BurnRecordsNewHead.onNewBlock(blockDb),
  ]);

  logPerf("adding block to leaderboards", tStartAnalyze);

  Log.debug(`store block seq queue ${newBlockQueue.size}`);
  const allBlocksProcessed =
    newBlockQueue.size === 0 &&
    // This function is on this queue.
    newBlockQueue.pending <= 1;

  if (allBlocksProcessed) {
    await updateDerivedBlockStats(block)();
    await notifyNewDerivedStats(block)();
  } else {
    Log.debug("blocks left to process, skipping computation of derived stats");
  }
};

export const onNewBlock = async (head: Head): Promise<void> =>
  newBlockQueue.add(() => addBlock(head));

const updateDerivedBlockStats = (block: BlockLondon) => {
  Log.debug("updating derived stats");
  const t0 = performance.now();

  const feesBurned = pipe(
    calcBaseFeeSums(block),
    T.chainFirstIOK(logPerfT("calc base fee sums", t0)),
  );

  const burnRates = pipe(
    calcBurnRates(block),
    T.chainFirstIOK(logPerfT("calc burn rates", t0)),
  );

  const leaderboardAllTask = async () => {
    const leaderboardAll = await LeaderboardsAll.calcLeaderboardAll();
    logPerfT("calc leaderboard all", t0);
    return leaderboardAll;
  };

  const leaderboardLimitedTimeframes = pipe(
    LeaderboardsLimitedTimeframe.calcLeaderboardForLimitedTimeframes(),
    T.chainFirstIOK(logPerfT("calc leaderboard limited timeframes", t0)),
  );

  // const burnRecords = BurnRecords.getRecords();

  const leaderboards: T.Task<LeaderboardEntries> = pipe(
    TAlt.seqTParT(leaderboardLimitedTimeframes, leaderboardAllTask),
    T.map(([leaderboardLimitedTimeframes, leaderboardAll]) => ({
      leaderboard5m: leaderboardLimitedTimeframes["5m"],
      leaderboard1h: leaderboardLimitedTimeframes["1h"],
      leaderboard24h: leaderboardLimitedTimeframes["24h"],
      leaderboard7d: leaderboardLimitedTimeframes["7d"],
      leaderboard30d: leaderboardLimitedTimeframes["30d"],
      leaderboardAll: leaderboardAll,
    })),
  );

  return pipe(
    TAlt.seqSParT({ burnRates, feesBurned, leaderboards }),
    T.chain(({ burnRates, feesBurned, leaderboards }) =>
      DerivedBlockStats.storeDerivedBlockStats({
        blockNumber: block.number,
        burnRates,
        feesBurned,
        leaderboards,
        // burnRecords,
      }),
    ),
    T.chainFirstIOK(() => () => {
      DerivedBlockStats.deleteOldDerivedStats()();
    }),
  );
};

const notifyNewDerivedStats = (block: BlockLondon): T.Task<void> => {
  const payload: NewBlockPayload = {
    number: block.number,
  };

  return pipe(
    () => sql.notify("new-derived-stats", JSON.stringify(payload)),
    T.map(() => undefined),
  );
};
