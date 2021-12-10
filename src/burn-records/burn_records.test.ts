import { suite, test } from "uvu";
import * as Duration from "../duration.js";
import * as BlocksData from "../blocks_data.js";
import * as assert from "uvu/assert";
import {
  addBlock,
  addBlockToState,
  FeeBlock,
  feeBlockFromBlock,
  Granularity,
  RecordState,
  Sorting,
  Sum,
} from "./burn_records.js";
import * as BurnRecords from "./burn_records.js";
import _ from "lodash";
import { denominations } from "../denominations.js";
import makeEta from "simple-eta";
import { BlockDb, FeeBlockRow } from "../blocks/blocks.js";
import { OrdM } from "../fp.js";
import { timeframeMinutesMap } from "../leaderboards.js";
import { TimeFrame } from "../time_frame.js";

(BigInt.prototype as any).toJSON = function () {
  return this.toString();
};

const makeRecordState = (): RecordState => ({
  denomination: "eth",
  feeBlocks: [],
  feeBlockRollbackBuffer: [],
  sumsRollbackBuffer: [],
  sums: [],
  topSums: [],
});

type StateInstruction =
  | {
      type: "add";
      block: BlockDb;
    }
  | { type: "rollback" };

const advanceState = (
  granularity: Granularity,
  instructions: StateInstruction[],
  sorting: Sorting = "max",
  timeFrame: TimeFrame = "5m",
  recordState: RecordState = makeRecordState(),
): RecordState =>
  instructions.reduce((state, instruction) => {
    if (instruction.type === "add") {
      return addBlockToState(
        state,
        instruction.block,
        granularity,
        "eth",
        sorting,
        timeFrame,
      );
    }

    if (instruction.type === "rollback") {
      return BurnRecords.rollbackBlock(state, granularity, timeFrame, sorting);
    }

    throw new Error("didn't know how to process state instruction");
  }, recordState);

const makeAdd = (block: BlockDb): StateInstruction => ({ type: "add", block });
const makeAdds = (blocks: BlockDb[]): StateInstruction[] =>
  blocks.map((block) => ({
    type: "add",
    block,
  }));

const makeRollbacks = (n: number): StateInstruction[] =>
  new Array(n).fill(null).map(() => ({ type: "rollback" }));

test("a new block results in a new fee block", async () => {
  const block = await BlocksData.getSingleBlock();
  const newState = advanceState("m5", [makeAdd(block)]);

  const [lastFeeBlock] = newState.feeBlocks;
  assert.is(lastFeeBlock?.number, block.number);
});

test("a new block results in a new sum", async () => {
  const block = await BlocksData.getSingleBlock();
  const feeBlock = feeBlockFromBlock("eth", block);
  const finalState = advanceState("m5", [makeAdd(block)]);

  const [lastSum] = finalState.sums;
  const expectedSum: Sum = {
    sum: feeBlock.fees,
    start: feeBlock.number,
    end: feeBlock.number,
    startMinedAt: feeBlock.minedAt,
  };

  assert.equal(lastSum, expectedSum);
});

test("block granularity adds at most one fee block", async () => {
  const m5Blocks = await BlocksData.getM5Blocks();
  const blocks = m5Blocks.slice(1, 3);
  const finalState = advanceState("block", makeAdds(blocks));

  const lastFeeBlock = _.last(finalState.feeBlocks);

  const expectedFees: FeeBlock = BurnRecords.feeBlockFromBlock(
    "eth",
    blocks[1],
  );

  assert.is(finalState.feeBlocks.length, 1);
  assert.equal(lastFeeBlock, expectedFees);
});

test("adding two blocks results in a sum of the two", async () => {
  const m5Blocks = await BlocksData.getM5Blocks();
  const blocks = m5Blocks.slice(1, 3);
  const newState = advanceState("m5", makeAdds(blocks));
  const lastSum = _.last(newState.sums);
  const feeBlocksExpected = blocks.map((block) =>
    BurnRecords.feeBlockFromBlock("eth", block),
  );

  const expectedSum = {
    sum: BurnRecords.sumFeeBlocks(feeBlocksExpected),
    start: blocks[0].number,
    end: blocks[1].number,
    startMinedAt: blocks[0].minedAt,
  };

  assert.equal(lastSum, expectedSum);
});

test("a new sum is added within the topSums limit", async () => {
  const block = await BlocksData.getSingleBlock();
  const feeBlock = feeBlockFromBlock("eth", block);
  const finalState = advanceState("m5", [makeAdd(block)]);

  const [lastSum] = finalState.topSums;
  const expectedSum: Sum = {
    sum: feeBlock.fees,
    start: feeBlock.number,
    end: feeBlock.number,
    startMinedAt: feeBlock.minedAt,
  };

  assert.equal(lastSum, expectedSum);
});

test("finds the proper matching index for a sum in topSums", async () => {
  const block = await BlocksData.getSingleBlock();
  const sum = BurnRecords.makeNewSum("eth", undefined, block);
  const lowerSum = { ...sum, sum: sum.sum - 1n };
  const higherSum = { ...sum, sum: sum.sum + 1n };
  const index = BurnRecords.getMatchingSumIndexFromRight(
    "max",
    [higherSum, lowerSum],
    sum,
  );

  assert.is(index, 1);
});

test("returns undefined when no matching index is found for a sum in topSums", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);
  const sums = blocks.map((block) =>
    BurnRecords.makeNewSum("eth", undefined, block),
  );

  const index = BurnRecords.getMatchingSumIndexFromRight("max", [], sums[0]);
  assert.is(index, undefined);

  const index2 = BurnRecords.getMatchingSumIndexFromRight(
    "max",
    [sums[1]],
    sums[0],
  );
  assert.is(index2, undefined);
});

test("a new sum is not added outside the topSums limit", async () => {
  const block = {
    ...(await BlocksData.getSingleBlock()),
    baseFeePerGas: 1n * 10n ** 18n,
  };
  const topSumsMaxCount = BurnRecords.getTopSumsMaxCount("block");
  const blocks = new Array(topSumsMaxCount).fill(block);

  const zeroFeeBlock = {
    ...block,
    number: blocks[0].number + 1,
    baseFeePerGas: 0n,
  };
  const finalState = advanceState("block", [
    ...makeAdds(blocks),
    makeAdd(zeroFeeBlock),
  ]);

  const lastTopSum = _.last(finalState.topSums)!;
  assert.is(finalState.topSums.length, topSumsMaxCount);
  assert.is.not(lastTopSum.end, zeroFeeBlock.number);
});

test("a record setting block makes it into topSums", async () => {
  const blocks = await BlocksData.getH1Blocks();
  const topSumsMaxCount = BurnRecords.getTopSumsMaxCount("block");

  assert.ok(topSumsMaxCount < blocks.length);

  const highFeeBlock = {
    ..._.last(blocks)!,
    number: _.last(blocks)!.number + 1,
    baseFeePerGas: 1n * 10n ** 18n,
  };

  const finalState = advanceState("block", [
    ...makeAdds(blocks),
    makeAdd(highFeeBlock),
  ]);

  assert.is(finalState.topSums[0].end, highFeeBlock.number);
});

test("sorting max sorts top records in descending order", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 3);
  const finalState = advanceState("block", makeAdds(blocks), "max");
  const first = _.head(finalState.topSums)!;
  const last = _.last(finalState.topSums)!;
  assert.ok(first.sum > last.sum);
});

test("sorting min sorts top records in ascending order", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 3);
  const finalState = advanceState("block", makeAdds(blocks), "min");
  const first = _.head(finalState.topSums)!;
  const last = _.last(finalState.topSums)!;

  assert.ok(first.sum < last.sum);
});

test("should create a correct block topSumsMaxCount", () => {
  const minBlockDuration = Duration.millisFromSeconds(12);
  const millisPerBlock = minBlockDuration;
  const count =
    (BurnRecords.recordsCount * millisPerBlock +
      BurnRecords.rollbackBufferMillis) /
    minBlockDuration;

  assert.is(BurnRecords.getTopSumsMaxCount("block"), count);
});

test("should create a correct m5 topSumsMaxCount", () => {
  const minBlockDuration = Duration.millisFromSeconds(12);
  const millisPer5Min = Duration.millisFromMinutes(5);
  const count =
    (BurnRecords.recordsCount * millisPer5Min +
      BurnRecords.rollbackBufferMillis) /
    minBlockDuration;

  assert.is(BurnRecords.getTopSumsMaxCount("m5"), count);
});

test("computes records from top sums", async () => {
  const blocks = await BlocksData.getM5Blocks();
  const finalState = advanceState("block", makeAdds(blocks), "max");
  const records = BurnRecords.getRecords(finalState);
  const first = _.head(records)!;
  const last = _.last(records)!;
  assert.is(first.end, 13666171);
  assert.is(last.end, 13666172);
});

test("recognizes left-side overlap", async () => {
  const sumA: Sum = {
    start: 0,
    end: 2,
    sum: 0n,
    startMinedAt: new Date(),
  };

  const sumB: Sum = {
    start: 1,
    end: 3,
    sum: 0n,
    startMinedAt: new Date(),
  };

  assert.ok(BurnRecords.getIsOverlapping([sumA], sumB));
});

test("recognizes left-side overlap", async () => {
  const sumA: Sum = {
    start: 0,
    end: 2,
    sum: 0n,
    startMinedAt: new Date(),
  };

  const sumB: Sum = {
    start: 1,
    end: 3,
    sum: 0n,
    startMinedAt: new Date(),
  };

  assert.ok(BurnRecords.getIsOverlapping([sumA], sumB));
});

test("recognizes right-side overlap", async () => {
  const sumA: Sum = {
    start: 1,
    end: 3,
    sum: 0n,
    startMinedAt: new Date(),
  };

  const sumB: Sum = {
    start: 0,
    end: 2,
    sum: 0n,
    startMinedAt: new Date(),
  };

  assert.ok(BurnRecords.getIsOverlapping([sumA], sumB));
});

test("rollback removes a block from the fee blocks set for block", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);
  const finalState = advanceState("block", [
    ...makeAdds(blocks),
    ...makeRollbacks(1),
  ]);

  assert.is(finalState.feeBlocks.length, 1);
});

test("rollback removes a block from the fee blocks set for m5", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);
  const state1 = advanceState("m5", makeAdds(blocks));

  assert.is(state1.feeBlocks.length, 2);

  const state2 = advanceState("m5", makeRollbacks(1), "max", "1h", state1);

  assert.is(state2.feeBlocks.length, 1);
});

test("rollback removes a sum from the sums", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);
  const state1 = advanceState("m5", makeAdds(blocks));

  assert.is(state1.sums.length, 2);

  const state2 = advanceState("m5", makeRollbacks(1), "max", "1h", state1);

  assert.is(state2.sums.length, 1);
});

test("rollback removes the reverted sum from top sums", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);

  const state1 = advanceState("m5", makeAdds(blocks));
  const first1 = _.head(state1.topSums)!;
  assert.is(first1?.end, 13666165);

  const state2 = advanceState("m5", makeRollbacks(1), "max", "1h", state1);
  const first2 = _.head(state2.topSums)!;
  assert.is.not(first2?.end, 13666165);
});

test("rollback for 'block' granularity restores the previous fee block", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);

  const state1 = advanceState("block", makeAdds(blocks));
  const last1 = _.last(state1.feeBlocks)!;
  assert.is(last1.number, blocks[1].number);

  const state2 = advanceState("block", makeRollbacks(1), "max", "1h", state1);
  const last2 = _.last(state2.feeBlocks)!;
  assert.is(last2.number, blocks[0].number);
});

test("rollback for timed granularities restore fee blocks within graularity", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);

  const state1 = advanceState("m5", makeAdds(blocks));
  const last1 = _.last(state1.feeBlocks)!;
  assert.is(last1.number, blocks[1].number);

  const state2 = advanceState("m5", makeRollbacks(1), "max", "1h", state1);
  const last2 = _.last(state2.feeBlocks)!;
  assert.is(last2.number, blocks[0].number);
});

test("merge candidate adds candidates under limit in sorted order", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 3);
  const sums = blocks.map((block) =>
    BurnRecords.makeNewSum("eth", undefined, block),
  );

  const topSums = sums.reduce(
    (topSums, sum) =>
      BurnRecords.mergeCandidate2("max", "block", topSums, sum).topSums,
    [] as Sum[],
  );

  assert.is(topSums[0].end, 13666165);
  assert.is(topSums[1].end, 13666166);
  assert.is(topSums[2].end, 13666164);
});

test("merge candidate respects topSumsMaxCount", async () => {
  const topSumsMaxCount = BurnRecords.getTopSumsMaxCount("block");
  const blocks = (await BlocksData.getH1Blocks()).slice(0, topSumsMaxCount + 1);
  const sums = blocks.map((block) =>
    BurnRecords.makeNewSum("eth", undefined, block),
  );
  const topSums = sums.reduce(
    (topSums, sum) =>
      BurnRecords.mergeCandidate2("max", "block", topSums, sum).topSums,
    [] as Sum[],
  );

  assert.is(topSums.length, topSumsMaxCount);
});

test("merge candidate doesn't add worse candidates when at limit", async () => {
  const topSumsMaxCount = BurnRecords.getTopSumsMaxCount("block");
  const blocks = (await BlocksData.getH1Blocks()).slice(0, topSumsMaxCount + 1);
  const seedBlocks = blocks.slice(0, topSumsMaxCount);
  assert.ok(
    seedBlocks.length >= topSumsMaxCount,
    "need at least topSumsMaxCount blocks to test limit",
  );
  const sums = seedBlocks.map((block) =>
    BurnRecords.makeNewSum("eth", undefined, block),
  );

  const worseSum = {
    ...BurnRecords.makeNewSum("eth", undefined, blocks[topSumsMaxCount]),
    sum: 0n,
  };

  const topSums = [...sums, worseSum].reduce(
    (topSums, sum) =>
      BurnRecords.mergeCandidate2("max", "block", topSums, sum).topSums,
    [] as Sum[],
  );

  const containsWorseSum = topSums.some((sum) => sum.end === worseSum.end);

  assert.not(containsWorseSum, "top sums contained worseSum but shouldn't");
});

test("merge candidate adds better candidates when at limit", async () => {
  const topSumsMaxCount = BurnRecords.getTopSumsMaxCount("block");
  const blocks = (await BlocksData.getH1Blocks()).slice(0, topSumsMaxCount + 1);
  const seedBlocks = blocks.slice(0, topSumsMaxCount);
  assert.ok(
    seedBlocks.length >= topSumsMaxCount,
    "need at least topSumsMaxCount blocks to test limit",
  );
  const sums = seedBlocks.map((block) =>
    BurnRecords.makeNewSum("eth", undefined, block),
  );

  const bestSum = {
    ...BurnRecords.makeNewSum("eth", undefined, blocks[topSumsMaxCount]),
    sum: 100n * 10n ** 18n,
  };

  const topSums = [...sums, bestSum].reduce(
    (topSums, sum) =>
      BurnRecords.mergeCandidate2("max", "block", topSums, sum).topSums,
    [] as Sum[],
  );

  const containsWorseSum = topSums.some((sum) => sum.end === bestSum.end);

  assert.ok(containsWorseSum, "top sums did not contain bestSum but should");
});

test("for equal fee top sums the earlier one ranks higher", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 3);
  const seedBlock = blocks[1];
  const sameFeeBlock = {
    ...blocks[2],
    baseFeePerGas: blocks[1].baseFeePerGas,
    gasUsed: blocks[1].gasUsed,
  };

  const seedSum = BurnRecords.makeNewSum("eth", undefined, seedBlock);
  const sameFeeSum = BurnRecords.makeNewSum("eth", undefined, sameFeeBlock);
  const { topSums } = BurnRecords.mergeCandidate2(
    "max",
    "block",
    [seedSum],
    sameFeeSum,
  );

  assert.is(topSums[0]?.end, seedBlock.number);
  assert.is(topSums[1]?.end, sameFeeBlock.number);
});

test.skip("does not advance state for granularities bigger than time frames", async () => {});

test.run();
