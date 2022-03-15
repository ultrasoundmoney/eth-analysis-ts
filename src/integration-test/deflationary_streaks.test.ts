import test from "ava";
import * as Db from "../db.js";
import * as DeflationaryStreaks from "../deflationary_streaks.js";
import { A, NEA, O, OAlt, pipe, T } from "../fp.js";
import * as SamplesBlocks from "../samples/blocks.js";
import * as MockDb from "./mock_db.js";

test.before(() => Db.runMigrations());

test.after(() => Db.closeConnection());

test.afterEach(() => MockDb.resetTables()());

test("should restore a previous streak on rollback", (t) =>
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
        T.chain(() => DeflationaryStreaks.getStreakState(12965024, true)),
        T.map((state) => {
          t.is(state, 8);
        }),
        T.chain(() =>
          DeflationaryStreaks.analyzeNewBlocks(blockSets.blockThatBreaksStreak),
        ),
        T.chain(() => DeflationaryStreaks.getStreakState(12965025, true)),
        T.map((state) => {
          t.is(state, 0);
        }),
        T.chain(() =>
          DeflationaryStreaks.rollbackBlocks(blockSets.blockThatBreaksStreak),
        ),
        T.chain(() => DeflationaryStreaks.getStreakState(12965024, true)),
        T.map((state) => {
          t.is(state, 8);
        }),
      ),
    ),
  )());
