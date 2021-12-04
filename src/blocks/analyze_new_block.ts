import { fromUnixTime } from "date-fns";
import PQueue from "p-queue";
import { calcBlockFeeBreakdown } from "../base_fees.js";
import { calcBaseFeeSums } from "../base_fee_sums.js";
import { calcBurnRates } from "../burn_rates.js";
import { sql } from "../db.js";
import * as DerivedBlockStats from "../derived_block_stats.js";
import { millisFromMinutes } from "../duration.js";
import { EthPrice } from "../etherscan.js";
import { BlockLondon } from "../eth_node.js";
import { getEthPrice, getPriceForOldBlock } from "../eth_prices.js";
import { B, E, pipe, T, TAlt, TE, TEAlt } from "../fp.js";
import { LeaderboardEntries } from "../leaderboards.js";
import * as Leaderboards from "../leaderboards.js";
import * as LeaderboardsAll from "../leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "../leaderboards_limited_timeframe.js";
import * as Log from "../log.js";
import { logPerfT } from "../performance.js";
import { getTxrsWithRetry } from "../transactions.js";
import * as Blocks from "./blocks.js";
import { NewBlockPayload } from "./blocks.js";
import * as BurnRecordsAll from "../burn-records/all.js";
import * as BurnRecordsLimitedTimeFrames from "../burn-records/limited_time_frames.js";

export const newBlockQueue = new PQueue({
  concurrency: 1,
  autoStart: false,
});

export const analyzeNewBlock = (blockNumber: number): T.Task<void> =>
  pipe(
    () => Log.debug(`analyzing block ${blockNumber}`),
    T.fromIO,
    T.chain(() => () => Blocks.getBlockWithRetry(blockNumber)),
    T.chainFirst((block) =>
      pipe(
        Blocks.getBlockHashIsKnown(block.parentHash),
        T.chain(
          B.match(
            // We're missing the parent hash, update the previous block.
            () =>
              pipe(
                () =>
                  Log.warn(
                    "storeNewBlock, parent hash not found, storing parent again",
                  ),
                () => analyzeNewBlock(blockNumber - 1),
              ),
            () => T.of(undefined),
          ),
        ),
      ),
    ),
    T.chain((block) =>
      TAlt.seqTParT(T.of(block), Blocks.getIsKnownBlock(block.number)),
    ),
    T.chain(([block, isKnownBlock]) =>
      TAlt.seqSParT({
        block: T.of(block),
        isKnownBlock: T.of(isKnownBlock),
        txrs: () => getTxrsWithRetry(block),
        ethPrice: pipe(
          getEthPrice(fromUnixTime(block.timestamp), millisFromMinutes(5)),
          TE.alt(
            (): TE.TaskEither<string, EthPrice> =>
              pipe(getPriceForOldBlock(block), T.map(E.right)),
          ),
          TEAlt.getOrThrow,
        ),
      }),
    ),
    T.chainFirst(({ block, isKnownBlock, txrs, ethPrice }) =>
      pipe(
        isKnownBlock,
        B.match(
          () => Blocks.storeBlock(block, txrs, ethPrice.ethusd),
          // Rollback
          () =>
            pipe(
              () => rollback(block),
              T.chain(() => Blocks.updateBlock(block, txrs, ethPrice.ethusd)),
            ),
        ),
      ),
    ),
    T.chainFirst(({ block, txrs, ethPrice }) => {
      const feeBreakdown = calcBlockFeeBreakdown(block, txrs, ethPrice.ethusd);

      const blockDb = Blocks.blockDbFromBlock(block, txrs, ethPrice.ethusd);

      const t0 = performance.now();

      LeaderboardsLimitedTimeframe.addBlockForAllTimeframes(
        blockDb,
        feeBreakdown.contract_use_fees,
        feeBreakdown.contract_use_fees_usd!,
      );

      const removeExpiredBlocksTask =
        LeaderboardsLimitedTimeframe.removeExpiredBlocksFromSumsForAllTimeframes();

      const addToLeaderboardAllTask = LeaderboardsAll.addBlock(
        block.number,
        feeBreakdown.contract_use_fees,
        feeBreakdown.contract_use_fees_usd!,
      );

      return pipe(
        TAlt.seqTParT(
          removeExpiredBlocksTask,
          addToLeaderboardAllTask,
          () => BurnRecordsAll.onNewBlock(blockDb),
          () => BurnRecordsLimitedTimeFrames.onNewBlock(blockDb),
        ),
        T.chainFirstIOK(logPerfT("adding block to leaderboards", t0)),
      );
    }),
    T.chain(({ block }) => {
      Log.debug(`store block seq queue ${newBlockQueue.size}`);
      const allBlocksProcessed =
        newBlockQueue.size === 0 &&
        // This function is on this queue.
        newBlockQueue.pending <= 1;

      if (!allBlocksProcessed) {
        Log.debug(
          "blocks left to process, skipping computation of derived stats",
        );
        return T.of(undefined);
      }

      return pipe(
        updateDerivedBlockStats(block),
        T.chain(() => notifyNewDerivedStats(block)),
      );
    }),
  );

const rollback = async (block: BlockLondon): Promise<void> => {
  const t0 = performance.now();

  Log.info(`rolling back block: ${block.number}`);

  const sumsToRollback = await Leaderboards.getRangeBaseFees(
    block.number,
    block.number,
  )();
  LeaderboardsLimitedTimeframe.rollbackToBefore(block.number, sumsToRollback);
  await Promise.all([
    LeaderboardsAll.removeContractBaseFeeSums(sumsToRollback)(),
    BurnRecordsAll.onRollback(block.number),
    BurnRecordsLimitedTimeFrames.onRollback(block.number),
  ]);
  logPerfT("rollback", t0);
};

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
  const leaderboardAll = pipe(
    LeaderboardsAll.calcLeaderboardAll(),
    T.chainFirstIOK(logPerfT("calc leaderboard all", t0)),
  );
  const leaderboardLimitedTimeframes = pipe(
    LeaderboardsLimitedTimeframe.calcLeaderboardForLimitedTimeframes(),
    T.chainFirstIOK(logPerfT("calc leaderboard limited timeframes", t0)),
  );
  const leaderboards: T.Task<LeaderboardEntries> = pipe(
    TAlt.seqTParT(leaderboardLimitedTimeframes, leaderboardAll),
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
