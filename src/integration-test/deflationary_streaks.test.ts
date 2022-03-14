import { test } from "uvu";
import * as assert from "uvu/assert";
import * as Db from "../db.js";
import * as DeflationaryStreaks from "../deflationary_streaks.js";
import { A, NEA, O, OAlt, pipe, T } from "../fp.js";
import * as SamplesBlocks from "../samples/blocks.js";
import * as MockDb from "./mock_db.js";

test.before(async () => {
  await Db.runMigrations();
});

test.after(async () => {
  await Db.closeConnection();
});

test.after.each(() => MockDb.resetTables()());

test("should restore a previous streak on rollback", async () =>
  pipe(
    MockDb.seedBlocks("h1", false),
    T.chain(() => SamplesBlocks.getBlocksFromFile("h1")),
    T.map((blocksH1) => {
      const blocksThatBuildStreak = pipe(
        blocksH1,
        A.filter((block) => block.number <= 12965024),
        NEA.fromArray,
        OAlt.getOrThrow("expected non empty list of blocks"),
      );
      const blockThatBreaksStreak = pipe(
        blocksH1,
        A.findFirst((block) => block.number === 12965025),
        O.map(NEA.of),
        OAlt.getOrThrow("expected block 12965025 in sample"),
      );
      return { blocksThatBuildStreak, blockThatBreaksStreak };
    }),
    T.chain((blockSets) =>
      pipe(
        DeflationaryStreaks.analyzeNewBlocks(blockSets.blocksThatBuildStreak),
        T.chain(() =>
          pipe(
            DeflationaryStreaks.getStreakState(12965024, true),
            T.map((state) => {
              assert.is(state, 9);
            }),
          ),
        ),
        T.chain(() =>
          pipe(
            DeflationaryStreaks.analyzeNewBlocks(
              blockSets.blockThatBreaksStreak,
            ),
            T.chain(() => DeflationaryStreaks.getStreakState(12965025, true)),
            T.map((state) => {
              assert.is(state, 1);
            }),
          ),
        ),
        T.chain(() =>
          pipe(
            DeflationaryStreaks.rollbackBlocks(blockSets.blockThatBreaksStreak),
            T.chain(() =>
              pipe(
                DeflationaryStreaks.getStreakState(12965024, true),
                T.map((state) => {
                  assert.equal(state, 9);
                }),
              ),
            ),
          ),
        ),
      ),
    ),
  )());

test.run();
