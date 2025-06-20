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
) =>
  pipe(
    Db.sqlT<{ blockNumber: number; count: number | null }[]>`
      SELECT block_number, count FROM deflationary_streaks
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
): T.Task<StreakForSite> =>
  pipe(
    T.Do,
    T.apS("preMerge", getStreakForSiteWithMergeState(block, false)),
    T.apS("postMerge", getStreakForSiteWithMergeState(block, true)),
  );

export const getStreak = (blockNumber: number, postMerge: boolean) =>
  pipe(
    Db.sqlT<{ count: number | null }[]>`
      SELECT count FROM deflationary_streaks
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
) =>
  Db.sqlTVoid`
    INSERT INTO deflationary_streaks
      ${Db.values({
        block_number: block.number,
        count: count,
        post_merge: postMerge,
      })}
  `;

export const getLastAnalyzed = () =>
  pipe(
    Db.sqlT<{ max: number }[]>`
      SELECT MAX(block_number) FROM deflationary_streaks
    `,
    T.map(flow((rows) => rows[0]?.max, O.fromNullable)),
  );

export const getNextBlockToAdd = () =>
  pipe(
    T.Do,
    T.apS("lastStoredBlock", Blocks.getLastStoredBlock()),
    T.apS("lastAnalyzed", getLastAnalyzed()),
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
};

const getBarrier = async () => {
  const resE = await Fetch.fetchJson(
    "https://ultrasound.money/api/v2/fees/base-fee-per-gas-stats",
  )();

  if (E.isLeft(resE)) {
    throw resE.left;
  }

  const baseFeePerGasStats = resE.right as BaseFeePerGasStats;
  return baseFeePerGasStats.barrier;
};

type GweiNumber = number;

const getIsAboveBarrier = (barrier: GweiNumber, block: Blocks.BlockV1) =>
  EthUnits.gweiFromWei(Number(block.baseFeePerGas)) > barrier;

const analyzeNewBlocksWithMergeState = (
  barrier: number,
  blocksToAdd: NEA.NonEmptyArray<Blocks.BlockV1>,
  postMerge: boolean,
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
              getStreak(block.number - 1, postMerge),
              T.map((streak) => streak + 1),
            ),
        ),
        T.chain((streak) => storeStreak(block, postMerge, streak)),
      ),
    ),
    TAlt.concatAllVoid,
  );

export const analyzeNewBlocks = (
  blocksToAdd: NEA.NonEmptyArray<Blocks.BlockV1>,
) =>
  pipe(
    () => getBarrier(),
    T.chainFirstIOK((barrier) => Log.debugIO(`barrier: ${barrier}`)),
    T.chain((barrier) =>
      pipe(
        TAlt.seqTPar(
          analyzeNewBlocksWithMergeState(barrier, blocksToAdd, false),
          analyzeNewBlocksWithMergeState(barrier, blocksToAdd, true),
        ),
        TAlt.concatAllVoid,
      ),
    ),
  );

export const rollbackBlocks = (
  blocksToRollback: NEA.NonEmptyArray<Blocks.BlockV1>,
) =>
  pipe(
    blocksToRollback,
    A.map((block) => block.number),
    (blockNumbers) => Db.sqlTVoid`
      DELETE FROM deflationary_streaks
      WHERE block_number IN (${blockNumbers})
    `,
  );
