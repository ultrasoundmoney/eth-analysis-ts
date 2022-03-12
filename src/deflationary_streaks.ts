import QuickLRU from "quick-lru";
import * as Blocks from "./blocks/blocks.js";
import { sql, sqlT, sqlTVoid } from "./db.js";
import * as EthUnits from "./eth_units.js";
import { A, B, flow, NEA, O, OAlt, pipe, T, TO } from "./fp.js";
import * as Log from "./log.js";
import * as StaticEtherData from "./static-ether-data.js";

type DeflationaryStreak = {
  from: Date;
  count: number;
};

export type DeflationaryStreakState = O.Option<DeflationaryStreak>;
export type DeflationaryStreakForSite = {
  preMerge: DeflationaryStreak | null;
  postMerge: DeflationaryStreak | null;
};

const getStorageKey = (postMerge: boolean) =>
  postMerge
    ? "deflationary-streak-post-merge"
    : "deflationary-streak-pre-merge";

const analysisStateKey = "deflationary-streaks";

export const getStreakStateForSite = (): T.Task<DeflationaryStreakForSite> =>
  pipe(
    T.Do,
    T.apS(
      "preMerge",
      pipe(
        sqlT<{ value: { from: Date; count: number } | null }[]>`
          SELECT value FROM key_value_store
          WHERE key = ${getStorageKey(false)}
        `,
        T.map((rows) => rows[0]?.value ?? null),
      ),
    ),
    T.apS(
      "postMerge",
      pipe(
        sqlT<{ value: { from: Date; count: number } | null }[]>`
          SELECT value FROM key_value_store
          WHERE key = ${getStorageKey(true)}
        `,
        T.map((rows) => rows[0]?.value ?? null),
      ),
    ),
  );

const getStreakState = (storageKey: string) =>
  pipe(
    sqlT<{ value: { from: Date; count: number } | null }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${storageKey}
    `,
    T.map(flow((rows) => rows[0]?.value, O.fromNullable)),
  );

const storeStreakState = (
  storageKey: string,
  streakState: DeflationaryStreakState,
) =>
  sqlTVoid`
    INSERT INTO key_value_store
      ${sql({
        key: storageKey,
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
      WHERE key = ${analysisStateKey}
    `,
    T.map(flow((rows) => rows[0]?.last, O.fromNullable)),
  );

const setLastAnalyzed = (blockNumber: number) => sqlTVoid`
  INSERT INTO analysis_state
    ${sql({
      key: analysisStateKey,
      last: blockNumber,
    })}
  ON CONFLICT (key) DO UPDATE SET
    last = excluded.last
`;

type RecentStreaks = QuickLRU<number, DeflationaryStreak>;
const recentpreMergeStreaks: RecentStreaks = new QuickLRU({
  maxSize: 8,
});
const recentpostMergeStreaks: RecentStreaks = new QuickLRU({
  maxSize: 8,
});
const getRecentStreaks = (postMerge: boolean) =>
  postMerge ? recentpostMergeStreaks : recentpreMergeStreaks;
const getRecentStreak = (recentStreaks: RecentStreaks, block: Blocks.BlockDb) =>
  pipe(recentStreaks.get(block.number), O.fromNullable);

export const getNextBlockToAdd = () =>
  pipe(
    getLastAnalyzed(),
    TO.matchE(
      () =>
        pipe(
          Blocks.getLastStoredBlock(),
          T.map((block) => block.number),
        ),
      (lastAnalyzed) => T.of(lastAnalyzed + 1),
    ),
  );

const getIsDeflationaryBlock = (
  issuancePerBlock: number,
  block: Blocks.BlockDb,
) => EthUnits.ethFromWei(Number(block.baseFeeSum)) > issuancePerBlock;

const startNewStreak = (block: Blocks.BlockDb) =>
  pipe(
    Blocks.getPreviousBlock(block),
    T.map(
      flow(
        OAlt.getOrThrow(
          `block ${
            block.number
          } starts a new streak, need timestamp from block ${
            block.number - 1
          } but this block was not found`,
        ),
        (previousBlock) =>
          O.some({
            from: previousBlock.minedAt,
            count: 1,
          }),
      ),
    ),
  );

const addBlockToState = (
  recentStreaks: RecentStreaks,
  streakState: DeflationaryStreakState,
  block: Blocks.BlockDb,
  issuancePerBlock: number,
) =>
  pipe(
    getIsDeflationaryBlock(issuancePerBlock, block),
    B.match(
      () => TO.none,
      () =>
        pipe(
          streakState,
          O.match(
            () => startNewStreak(block),
            (state) =>
              TO.of({
                ...state,
                count: state.count + 1,
              }),
          ),
        ),
    ),
    // Remember the state for the block in case we need to roll back to it.
    TO.chainFirstIOK((state) => () => {
      recentStreaks.set(block.number, state);
    }),
  );

const addBlocksToState = (
  recentStreaks: RecentStreaks,
  streakState: DeflationaryStreakState,
  blocksToAdd: NEA.NonEmptyArray<Blocks.BlockDb>,
  issuancePerBlock: number,
) =>
  pipe(
    blocksToAdd,
    A.reduce(T.of(streakState), (state, block) =>
      pipe(
        state,
        T.chain((state) =>
          addBlockToState(recentStreaks, state, block, issuancePerBlock),
        ),
      ),
    ),
  );

// Removing happens on rollback. Because a rolled back block might have undone the previous state, it isn't always as simple as reducing any running streak by one. Therefore, on adding blocks, we remember what the previous state was, and restore it here.
const removeBlocksFromRecentStreaks = (
  recentStreaks: RecentStreaks,
  blocksToRemove: NEA.NonEmptyArray<Blocks.BlockDb>,
) =>
  pipe(
    blocksToRemove,
    NEA.sort(Blocks.sortDesc),
    NEA.last,
    (block) => getRecentStreak(recentStreaks, block),
    O.altW(() => {
      // This may happen when history is empty on start and we roll back immediately.
      Log.error(
        "tried to restore the previous block's deflationary streak, but none found in recent history",
      );
      return O.none;
    }),
  );

const getIssuancePerBlock = (postMerge: boolean) =>
  postMerge
    ? StaticEtherData.issuancePerBlockPostMerge
    : StaticEtherData.issuancePerBlockPreMerge;

const analyzeNewBlocksWithMergeState = (
  blocksToAdd: NEA.NonEmptyArray<Blocks.BlockDb>,
  postMerge: boolean,
) =>
  pipe(
    getStreakState(getStorageKey(postMerge)),
    T.chain((streakState) =>
      addBlocksToState(
        getRecentStreaks(postMerge),
        streakState,
        blocksToAdd,
        getIssuancePerBlock(postMerge),
      ),
    ),
    T.chain((state) => storeStreakState(getStorageKey(postMerge), state)),
  );

export const analyzeNewBlocks = (
  blocksToAdd: NEA.NonEmptyArray<Blocks.BlockDb>,
) =>
  pipe(
    T.Do,
    T.apS("preMerge", analyzeNewBlocksWithMergeState(blocksToAdd, false)),
    T.apS("postMerge", analyzeNewBlocksWithMergeState(blocksToAdd, true)),
    T.chain(() => setLastAnalyzed(NEA.last(blocksToAdd).number)),
  );

const rollbackBlocksWithMergeState = (
  blocksToRollback: NEA.NonEmptyArray<Blocks.BlockDb>,
  postMerge: boolean,
) =>
  pipe(
    removeBlocksFromRecentStreaks(
      getRecentStreaks(postMerge),
      blocksToRollback,
    ),
    (state) => storeStreakState(getStorageKey(postMerge), state),
  );

export const rollbackBlocks = (
  blocksToRollback: NEA.NonEmptyArray<Blocks.BlockDb>,
) =>
  pipe(
    T.Do,
    T.apS("preMerge", rollbackBlocksWithMergeState(blocksToRollback, false)),
    T.apS("postMerge", rollbackBlocksWithMergeState(blocksToRollback, true)),
    T.chain(() => setLastAnalyzed(NEA.last(blocksToRollback).number - 1)),
  );
