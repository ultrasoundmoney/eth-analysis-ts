import test from "ava";
import * as Db from "../db.js";
import * as DeflationaryStreaks from "../deflationary_streaks.js";
import * as EthUnits from "../eth_units.js";
import { A, NEA, O, OAlt, pipe, T } from "../fp.js";
import * as SamplesBlocks from "../samples/blocks.js";
import * as MockDb from "./mock_db.js";

test.before(() => Db.runMigrations());

test.after(() => Db.closeConnection());

test.afterEach(() => MockDb.resetTables()());

test("should start a streak when no previous streaks exist", (t) =>
  pipe(
    MockDb.seedBlocks("m5", false),
    T.chain(() => SamplesBlocks.getBlocksFromFile("h1")),
    T.map((blocks) =>
      pipe(
        blocks,
        A.findFirst((block) => block.number === 12965017),
        OAlt.getOrThrow("expected block 12965017 in sample"),
        NEA.of,
      ),
    ),
    T.chain((blocks) =>
      pipe(
        DeflationaryStreaks.analyzeNewBlocks(blocks),
        T.chain(() => DeflationaryStreaks.getStreak(12965017, true)),
        T.map((streak) => {
          t.is(streak, 1);
        }),
      ),
    ),
  )());

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
        T.chain(() => DeflationaryStreaks.getStreak(12965024, true)),
        T.map((streak) => {
          t.is(streak, 8);
        }),
        T.chain(() =>
          DeflationaryStreaks.analyzeNewBlocks(blockSets.blockThatBreaksStreak),
        ),
        T.chain(() => DeflationaryStreaks.getStreak(12965025, true)),
        T.map((streak) => {
          t.is(streak, 0);
        }),
        T.chain(() =>
          DeflationaryStreaks.rollbackBlocks(blockSets.blockThatBreaksStreak),
        ),
        T.chain(() => DeflationaryStreaks.getStreak(12965024, true)),
        T.map((streak) => {
          t.is(streak, 8);
        }),
      ),
    ),
  )());

test("should add a pow block that is over the threshold", (t) =>
  pipe(
    MockDb.seedBlocks("h1", false),
    T.chain(() => SamplesBlocks.getBlocksFromFile("h1")),
    T.map((blocks) =>
      pipe(
        blocks,
        NEA.head,
        (block) => ({
          ...block,
          baseFeeSum: BigInt(EthUnits.weiFromEth(3)),
        }),
        NEA.of,
      ),
    ),
    T.chain((blocks) =>
      pipe(
        DeflationaryStreaks.analyzeNewBlocks(blocks),
        T.chain(() => DeflationaryStreaks.getStreak(12965000, false)),
        T.map((streak) => {
          t.is(streak, 1);
        }),
      ),
    ),
  )());

test("should not add a pow block that is not over the threshold", (t) =>
  pipe(
    MockDb.seedBlocks("h1", false),
    T.chain(() => SamplesBlocks.getBlocksFromFile("h1")),
    T.map((blocks) =>
      pipe(
        blocks,
        NEA.head,
        (block) => ({
          ...block,
          baseFeeSum: BigInt(EthUnits.weiFromEth(1)),
        }),
        NEA.of,
      ),
    ),
    T.chain((blocks) =>
      pipe(
        DeflationaryStreaks.analyzeNewBlocks(blocks),
        T.chain(() => DeflationaryStreaks.getStreak(12965000, false)),
        T.map((streak) => {
          t.is(streak, 0);
        }),
      ),
    ),
  )());

test("should add a pos block that is over the threshold", (t) =>
  pipe(
    MockDb.seedBlocks("h1", false),
    T.chain(() => SamplesBlocks.getBlocksFromFile("h1")),
    T.map((blocks) =>
      pipe(
        blocks,
        NEA.head,
        (block) => ({
          ...block,
          baseFeeSum: BigInt(EthUnits.weiFromEth(0.3)),
        }),
        NEA.of,
      ),
    ),
    T.chain((blocks) =>
      pipe(
        DeflationaryStreaks.analyzeNewBlocks(blocks),
        T.chain(() => DeflationaryStreaks.getStreak(12965000, true)),
        T.map((streak) => {
          t.is(streak, 1);
        }),
      ),
    ),
  )());
