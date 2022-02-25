import * as Blocks from "./blocks/blocks.js";
import { sql, sqlT, sqlTVoid } from "./db.js";
import * as EthUnits from "./eth_units.js";
import { A, flow, NEA, O, pipe, T } from "./fp.js";
import * as StaticEtherData from "./static-ether-data.js";

type DeflationaryStreak = {
  from: Date;
  count: number;
};

export type DeflationaryStreakState = O.Option<DeflationaryStreak>;

export const deflationaryStreakCacheKey = "deflationary-streak";
const analysisKey = "deflationaryStreakKey";

export const getStreakState = () =>
  pipe(
    sqlT<{ value: { from: Date; count: number } | null }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${deflationaryStreakCacheKey}
    `,
    T.map(flow((rows) => rows[0]?.value, O.fromNullable)),
  );

const storeStreakState = (streakState: DeflationaryStreakState) =>
  sqlTVoid`
    INSERT INTO key_value_store
      ${sql({
        key: deflationaryStreakCacheKey,
        value: pipe(
          streakState,
          O.match(
            () => null,
            (state) => JSON.stringify(state),
          ),
        ),
      })}
    ON CONFLICT (key) DO UPDATE SET
      value = excluded.value
  `;

export const getLastAnalyzed = () =>
  pipe(
    sqlT<{ last: number }[]>`
      SELECT last FROM analysis_state
      WHERE key = ${deflationaryStreakCacheKey}
    `,
    T.map(flow((rows) => rows[0]?.last, O.fromNullable)),
  );

const setLastAnalyzed = (blockNumber: number) => sqlTVoid`
  INSERT INTO analysis_state
    ${sql({
      key: analysisKey,
      last: blockNumber,
    })}
  ON CONFLICT (key) DO UPDATE SET
    last = excluded.last
`;

export const getNextBlockToAdd = () =>
  pipe(
    getLastAnalyzed(),
    T.chain(
      O.match(
        () =>
          pipe(
            Blocks.getLastStoredBlock(),
            T.map((block) => block.number),
          ),
        (lastAnalyzed) => T.of(lastAnalyzed + 1),
      ),
    ),
  );

const getIsDeflationaryBlock = (block: Blocks.BlockDb) =>
  Number(EthUnits.ethFromWeiBI(block.baseFeeSum)) >
  StaticEtherData.issuancePerBlock;

const addBlockToState = (
  streakState: DeflationaryStreakState,
  block: Blocks.BlockDb,
) =>
  getIsDeflationaryBlock(block)
    ? pipe(
        streakState,
        O.match(
          () =>
            O.some({
              from: block.minedAt,
              count: 1,
            }),
          (state) =>
            O.some({
              ...state,
              count: state.count + 1,
            }),
        ),
      )
    : O.none;

const addBlocksToState = (
  streakState: DeflationaryStreakState,
  blocksToAdd: NEA.NonEmptyArray<Blocks.BlockDb>,
) =>
  pipe(
    blocksToAdd,
    A.reduce(streakState, (state, block) => addBlockToState(state, block)),
  );

const removeBlockFromState = (streakState: DeflationaryStreakState) =>
  pipe(
    streakState,
    O.match(
      // We currently have no way to recover streaks, if no streak is running, we assume none was running, and stay in the no-streak state.
      () => O.none,
      (state) =>
        state.count === 1
          ? // This was the first block in the streak. We go back to the no-streak state.
            O.none
          : // There are multiple blocks in the current streak, decrement streak by one.
            O.some({ ...state, count: state.count - 1 }),
    ),
  );

const removeBlocksFromState = (
  streakState: DeflationaryStreakState,
  blocksToRemove: NEA.NonEmptyArray<Blocks.BlockDb>,
) =>
  pipe(
    blocksToRemove,
    A.reduce(streakState, (state) => removeBlockFromState(state)),
  );

export const analyzeNewBlocks = (
  blocksToAdd: NEA.NonEmptyArray<Blocks.BlockDb>,
) =>
  pipe(
    T.Do,
    T.apS("streakState", getStreakState()),
    T.map(({ streakState }) => addBlocksToState(streakState, blocksToAdd)),
    T.chain((state) => storeStreakState(state)),
    T.chain(() => setLastAnalyzed(NEA.last(blocksToAdd).number)),
  );

export const rollbackBlocks = (
  blocksToRollback: NEA.NonEmptyArray<Blocks.BlockDb>,
) =>
  pipe(
    getStreakState(),
    T.map((streakState) =>
      removeBlocksFromState(streakState, blocksToRollback),
    ),
    T.chain((state) => storeStreakState(state)),
    T.chain(() => setLastAnalyzed(NEA.last(blocksToRollback).number - 1)),
  );
