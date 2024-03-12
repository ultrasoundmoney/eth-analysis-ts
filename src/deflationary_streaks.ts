import * as Blocks from "./blocks/blocks.js";
import * as Db from "./db.js";
import * as EthUnits from "./eth_units.js";
import { A, B, E, flow, NEA, O, OAlt, pipe, T, TAlt, TO } from "./fp.js";
import * as Fetch from "./fetch.js";
import * as Log from "./log.js";

type Count = number;
export type Streak = O.Option<Count>;
export type StreakForSite = {
  preMerge: { count: number; startedOn: Date } | null;
  postMerge: { count: number; startedOn: Date } | null;
};

const getStreakForSiteWithMergeState = (
  block: Blocks.BlockV1,
  postMerge: boolean,
  blobStreak: boolean,
) =>
  pipe(
    Db.sqlT<{ blockNumber: number; count: number | null }[]>`
      SELECT block_number, count FROM ${
        blobStreak ? "deflationary_blob_streaks" : "deflationary_streaks"
      }
      WHERE block_number = ${block.number}
      AND post_merge = ${postMerge}
    `,
    T.map(
      flow(
        A.head,
        O.chain((row) =>
          row.count === null || row.count === 0
            ? O.none
            : O.some({
                count: row.count,
                // The start of any deflationary streak is defined as 12s before the first block in the streak.
                startedOnBlock: block.number - row.count,
              }),
        ),
      ),
    ),
    TO.chainTaskK(({ count, startedOnBlock }) =>
      pipe(
        Db.sqlT<{ minedAt: Date }[]>`
          SELECT mined_at FROM blocks
          WHERE number = ${startedOnBlock}
        `,
        T.map(
          flow(
            A.head,
            OAlt.getOrThrow(
              `expected block ${startedOnBlock} to exist to determine start of streak`,
            ),
            (row) => ({
              count: count,
              startedOn: row.minedAt,
            }),
          ),
        ),
      ),
    ),
    T.map(O.toNullable),
  );

export const getStreakForSite = (
  block: Blocks.BlockV1,
  blobStreak: boolean,
): T.Task<StreakForSite> =>
  pipe(
    T.Do,
    T.apS("preMerge", getStreakForSiteWithMergeState(block, false, blobStreak)),
    T.apS("postMerge", getStreakForSiteWithMergeState(block, true, blobStreak)),
  );

export const getStreak = (
  blockNumber: number,
  postMerge: boolean,
  blobStreak: boolean,
) =>
  pipe(
    Db.sqlT<{ count: number | null }[]>`
      SELECT count FROM ${
        blobStreak ? "deflationary_blob_streaks" : "deflationary_streaks"
      }
      WHERE block_number = ${blockNumber}
      AND post_merge = ${postMerge}
    `,
    T.map(
      flow(
        A.head,
        O.chainNullableK((row) => row.count),
        O.getOrElse(() => 0),
      ),
    ),
  );

const storeStreak = (
  block: Blocks.BlockV1,
  postMerge: boolean,
  count: number,
  blobStreak: boolean,
) =>
  Db.sqlTVoid`
    INSERT INTO ${
      blobStreak ? "deflationary_blob_streaks" : "deflationary_streaks"
    }
      ${Db.values({
        block_number: block.number,
        count: count,
        post_merge: postMerge,
      })}
  `;

export const getLastAnalyzed = (blobStreak: boolean) =>
  pipe(
    Db.sqlT<{ max: number }[]>`
      SELECT MAX(block_number) FROM ${
        blobStreak ? "deflationary_blob_streaks" : "deflationary_streaks"
      }
    `,
    T.map(flow((rows) => rows[0]?.max, O.fromNullable)),
  );

export const getNextBlockToAdd = (blobStreak: boolean) =>
  pipe(
    T.Do,
    T.apS("lastStoredBlock", Blocks.getLastStoredBlock()),
    T.apS("lastAnalyzed", getLastAnalyzed(blobStreak)),
    T.map(({ lastStoredBlock, lastAnalyzed }) =>
      pipe(
        lastAnalyzed,
        O.match(
          () => O.some(lastStoredBlock.number),
          (lastAnalyzed) =>
            lastAnalyzed === lastStoredBlock.number
              ? O.none
              : O.some(lastAnalyzed + 1),
        ),
      ),
    ),
  );

type GweiPerGas = number;

type BaseFeePerGasStats = {
  barrier: GweiPerGas;
  blob_barrier: GweiPerGas;
};

const getBarrier = async (blobStreak: boolean) => {
  Log.debug("getBarrier: ", blobStreak);
  const resE = await Fetch.fetchJson(
    "https://ultrasound.money/api/v2/fees/base-fee-per-gas-stats",
  )();

  if (E.isLeft(resE)) {
    throw resE.left;
  }

  const baseFeePerGasStats = resE.right as BaseFeePerGasStats;
  Log.debug("baseFeePerGasStats:", baseFeePerGasStats);
  return blobStreak
    ? baseFeePerGasStats.blob_barrier
    : baseFeePerGasStats.barrier;
};

type GweiNumber = number;

const getIsAboveBarrier = (barrier: GweiNumber, block: Blocks.BlockV1) =>
  EthUnits.gweiFromWei(Number(block.baseFeePerGas)) > barrier;

const analyzeNewBlocksWithMergeState = (
  barrier: number,
  blocksToAdd: NEA.NonEmptyArray<Blocks.BlockV1>,
  postMerge: boolean,
  blobStreak: boolean,
) =>
  pipe(
    blocksToAdd,
    T.traverseSeqArray((block) =>
      pipe(
        getIsAboveBarrier(barrier, block),
        B.match(
          () => T.of(0),
          () =>
            pipe(
              getStreak(block.number - 1, postMerge, blobStreak),
              T.map((streak) => streak + 1),
            ),
        ),
        T.chain((streak) => storeStreak(block, postMerge, streak, blobStreak)),
      ),
    ),
    TAlt.concatAllVoid,
  );

export const analyzeNewBlocks = (
  blocksToAdd: NEA.NonEmptyArray<Blocks.BlockV1>,
) =>
  TAlt.seqTPar(
    pipe(
      () => getBarrier(false),
      T.chainFirstIOK((barrier) => Log.debugIO(`barrier: ${barrier}`)),
      T.chain((barrier) =>
        pipe(
          TAlt.seqTPar(
            analyzeNewBlocksWithMergeState(barrier, blocksToAdd, false, false),
            analyzeNewBlocksWithMergeState(barrier, blocksToAdd, true, false),
          ),
          TAlt.concatAllVoid,
        ),
      ),
    ),
    pipe(
      () => getBarrier(true),
      T.chainFirstIOK((barrier) => Log.debugIO(`blob_barrier: ${barrier}`)),
      T.chain((barrier) =>
        pipe(
          TAlt.seqTPar(
            analyzeNewBlocksWithMergeState(barrier, blocksToAdd, false, true),
            analyzeNewBlocksWithMergeState(barrier, blocksToAdd, true, true),
          ),
          TAlt.concatAllVoid,
        ),
      ),
    ),
  );

export const rollbackBlocks = (
  blocksToRollback: NEA.NonEmptyArray<Blocks.BlockV1>,
  blobStreak: boolean,
) =>
  pipe(
    blocksToRollback,
    A.map((block) => block.number),
    (blockNumbers) => Db.sqlTVoid`
      DELETE FROM ${
        blobStreak ? "deflationary_blob_streaks" : "deflationary_streaks"
      }
      WHERE block_number IN (${blockNumbers})
    `,
  );
