import _ from "lodash";
import { suite, test } from "uvu";
import * as assert from "uvu/assert";
import { BlockDb } from "../blocks/blocks.js";
import * as BlocksData from "../blocks_data.js";
import * as BurnRecords from "./burn_records.js";
import { FeeBlock, RecordState, Sum } from "./burn_records.js";

(BigInt.prototype as any).toJSON = function () {
  return this.toString();
};

type StateInstruction =
  | {
      type: "add";
      block: BlockDb;
    }
  | { type: "rollback" };

const advanceState = (
  instructions: StateInstruction[],
  recordState?: RecordState,
): RecordState => {
  const initState = recordState || BurnRecords.makeRecordState("block", "5m");

  return instructions.reduce((state, instruction) => {
    if (instruction.type === "add") {
      return BurnRecords.addBlockToState(state, instruction.block);
    }

    if (instruction.type === "rollback") {
      return BurnRecords.rollbackBlock(state);
    }

    throw new Error("didn't know how to process state instruction");
  }, initState);
};

const makeAddUpdates = (blocks: BlockDb[]): StateInstruction[] =>
  blocks.map((block) => ({
    type: "add",
    block,
  }));

const makeRollbackUpdates = (n: number): StateInstruction[] =>
  new Array(n).fill(null).map(() => ({ type: "rollback" }));

const makeM5State = () => BurnRecords.makeRecordState("m5", "5m");

const FeeBlocks = suite("FeeBlocks");

FeeBlocks("a new block results in a new fee block", async () => {
  const block = await BlocksData.getSingleBlock();
  const newState = advanceState(makeAddUpdates([block]), makeM5State());

  const [lastFeeBlock] = newState.feeBlocks;
  assert.is(lastFeeBlock?.number, block.number);
});

FeeBlocks("a new block results in a new sum", async () => {
  const block = await BlocksData.getSingleBlock();
  const feeBlock = BurnRecords.feeBlockFromBlock(block);
  const finalState = advanceState(makeAddUpdates([block]), makeM5State());

  const [lastSum] = finalState.sums;
  const expectedSum: Sum = {
    end: feeBlock.number,
    endMinedAt: feeBlock.minedAt,
    start: feeBlock.number,
    startMinedAt: feeBlock.minedAt,
    sumEth: feeBlock.feesEth,
    sumUsd: feeBlock.feesUsd,
  };

  assert.equal(lastSum, expectedSum);
});

FeeBlocks("block granularity adds at most one fee block", async () => {
  const m5Blocks = await BlocksData.getM5Blocks();
  const blocks = m5Blocks.slice(1, 3);
  const finalState = advanceState(makeAddUpdates(blocks));

  const lastFeeBlock = _.last(finalState.feeBlocks);

  const expectedFees: FeeBlock = BurnRecords.feeBlockFromBlock(blocks[1]);

  assert.is(finalState.feeBlocks.length, 1);
  assert.equal(lastFeeBlock, expectedFees);
});

const Sums = suite("Sums");

Sums("adding two blocks results in a sum of the two", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(1, 3);
  const { sums } = advanceState(makeAddUpdates(blocks), makeM5State());
  const lastSum = _.last(sums);
  const feeBlocks = blocks.map((block) => BurnRecords.feeBlockFromBlock(block));

  const expectedSum = {
    end: blocks[1].number,
    endMinedAt: blocks[1].minedAt,
    start: blocks[0].number,
    startMinedAt: blocks[0].minedAt,
    sumEth: BurnRecords.sumFeeBlocks("eth", feeBlocks),
    sumUsd: BurnRecords.sumFeeBlocks("usd", feeBlocks),
  };

  assert.equal(lastSum, expectedSum);
});

Sums("finds the proper matching index for a sum in topSums", async () => {
  const block = await BlocksData.getSingleBlock();
  const sum = BurnRecords.makeNewSum(undefined, block);
  const lowerSum = { ...sum, sumEth: sum.sumEth - 1n };
  const higherSum = { ...sum, sumEth: sum.sumEth + 1n };
  const index = BurnRecords.getMatchingSumIndexFromRight(
    "eth",
    "max",
    [higherSum, lowerSum],
    sum,
  );

  assert.is(index, 1);
});

Sums(
  "returns undefined when no matching index is found for a sum in topSums",
  async () => {
    const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);
    const sums = blocks.map((block) =>
      BurnRecords.makeNewSum(undefined, block),
    );

    const index = BurnRecords.getMatchingSumIndexFromRight(
      "eth",
      "max",
      [],
      sums[0],
    );
    assert.is(index, undefined);

    const index2 = BurnRecords.getMatchingSumIndexFromRight(
      "eth",
      "max",
      [sums[1]],
      sums[0],
    );
    assert.is(index2, undefined);
  },
);

Sums.run();

const TopSums = suite("TopSums");

TopSums("a new sum is added to topSums", async () => {
  const block = await BlocksData.getSingleBlock();
  const feeBlock = BurnRecords.feeBlockFromBlock(block);
  const finalState = advanceState(makeAddUpdates([block]), makeM5State());

  const [lastSum] = finalState.topSumsMap["eth"]["max"];
  const expectedSum: Sum = {
    end: feeBlock.number,
    endMinedAt: feeBlock.minedAt,
    start: feeBlock.number,
    startMinedAt: feeBlock.minedAt,
    sumEth: feeBlock.feesEth,
    sumUsd: feeBlock.feesUsd,
  };

  assert.equal(lastSum, expectedSum);
});

TopSums("record setting block makes it into topSums", async () => {
  const blocks = await BlocksData.getH1Blocks();
  const topSumsMaxCount = BurnRecords.getTopSumsMaxCount("block");

  assert.ok(topSumsMaxCount < blocks.length);

  const highFeeBlock = {
    ..._.last(blocks)!,
    number: _.last(blocks)!.number + 1,
    baseFeePerGas: 1n * 10n ** 18n,
  };

  const { topSumsMap } = advanceState([
    ...makeAddUpdates(blocks),
    ...makeAddUpdates([highFeeBlock]),
  ]);

  assert.is(topSumsMap["eth"]["max"][0].end, highFeeBlock.number);
});

TopSums("sorting max sorts top records in descending order", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 3);
  const initState = BurnRecords.makeRecordState("block", "5m");
  const { topSumsMap } = advanceState(makeAddUpdates(blocks), initState);
  const first = _.head(topSumsMap["eth"]["max"])!;
  const last = _.last(topSumsMap["eth"]["max"])!;
  assert.ok(first.sumEth > last.sumEth);
});

TopSums("sorting min sorts top records in ascending order", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 3);
  const initState = BurnRecords.makeRecordState("block", "5m");
  const { topSumsMap } = advanceState(makeAddUpdates(blocks), initState);
  const first = _.head(topSumsMap["eth"]["min"])!;
  const last = _.last(topSumsMap["eth"]["min"])!;

  assert.ok(first.sumEth < last.sumEth);
});

TopSums("merge candidate adds candidates in sorted order", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 3);
  const sums = blocks.map((block) => BurnRecords.makeNewSum(undefined, block));

  const topSums = sums.reduce(
    (topSums, sum) => BurnRecords.mergeCandidate2("eth", "max", topSums, sum),
    [] as Sum[],
  );

  assert.is(topSums[0].end, 13666165);
  assert.is(topSums[1].end, 13666166);
  assert.is(topSums[2].end, 13666164);
});

TopSums("merge candidate drops expired sums from topSums", async () => {
  const blocks = (await BlocksData.getH1Blocks()).slice(0, 26);

  const { topSumsMap } = advanceState(makeAddUpdates(blocks));

  const containsExpiredSum = topSumsMap["eth"]["max"].some(
    (sum) => sum.end === 13666568,
  );

  assert.not(containsExpiredSum);
});

TopSums("for equal fee top sums the earlier one ranks higher", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 3);
  const seedBlock = blocks[1];
  const sameFeeBlock = {
    ...blocks[2],
    baseFeePerGas: blocks[1].baseFeePerGas,
    gasUsed: blocks[1].gasUsed,
  };

  const seedSum = BurnRecords.makeNewSum(undefined, seedBlock);
  const sameFeeSum = BurnRecords.makeNewSum(undefined, sameFeeBlock);
  const topSums = BurnRecords.mergeCandidate2(
    "eth",
    "max",
    [seedSum],
    sameFeeSum,
  );

  assert.is(topSums[0]?.end, seedBlock.number);
  assert.is(topSums[1]?.end, sameFeeBlock.number);
});

TopSums("top sums outside time frame get dropped", async () => {
  const blocks = await BlocksData.getH1Blocks();
  // We slice so that the first sums fall outside of the m5 time frame.
  const m5PlusBlocks = blocks.slice(0, 26);

  const outsideM5Sum = 13666568;

  const initState = makeM5State();
  const { topSumsMap } = advanceState(
    [...makeAddUpdates(m5PlusBlocks)],
    initState,
  );
  const topSums = topSumsMap["eth"]["max"];

  const containsExpiredSum = topSums.some((sum) => sum.end === outsideM5Sum);

  assert.not(containsExpiredSum, "topSums contains expired sum but shouldn't");
});

TopSums.run();

const Records = suite("Records");

Records("computes records from top sums", async () => {
  const blocks = await BlocksData.getM5Blocks();
  const { topSumsMap } = advanceState(makeAddUpdates(blocks));
  const topSums = topSumsMap["eth"]["max"];
  const records = BurnRecords.getRecords(topSums);
  const first = _.head(records)!;
  const last = _.last(records)!;
  assert.is(first.end, 13666171);
  assert.is(last.end, 13666172);
});

Records("recognizes left-side overlap", async () => {
  const sumA: Sum = {
    start: 0,
    end: 2,
    endMinedAt: new Date(),
    startMinedAt: new Date(),
    sumEth: 0n,
    sumUsd: 0n,
  };

  const sumB: Sum = {
    start: 1,
    end: 3,
    endMinedAt: new Date(),
    startMinedAt: new Date(),
    sumEth: 0n,
    sumUsd: 0n,
  };

  assert.ok(BurnRecords.getIsOverlapping([sumA], sumB));
});

Records("recognizes right-side overlap", async () => {
  const sumA: Sum = {
    start: 1,
    end: 3,
    endMinedAt: new Date(),
    startMinedAt: new Date(),
    sumEth: 0n,
    sumUsd: 0n,
  };

  const sumB: Sum = {
    start: 0,
    end: 2,
    endMinedAt: new Date(),
    startMinedAt: new Date(),
    sumEth: 0n,
    sumUsd: 0n,
  };

  assert.ok(BurnRecords.getIsOverlapping([sumA], sumB));
});

Records.run();

const Rollbacks = suite("Rollback");

Rollbacks(
  "rollback removes a block from the fee blocks set for block",
  async () => {
    const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);
    const { feeBlocks } = advanceState([
      ...makeAddUpdates(blocks),
      ...makeRollbackUpdates(1),
    ]);

    assert.is(feeBlocks.length, 1);
  },
);

Rollbacks(
  "rollback removes a block from the fee blocks set for m5",
  async () => {
    const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);
    const state1 = advanceState(makeAddUpdates(blocks), makeM5State());

    assert.is(state1.feeBlocks.length, 2);

    const state2 = advanceState(makeRollbackUpdates(1), state1);

    assert.is(state2.feeBlocks.length, 1);
  },
);

Rollbacks("rollback removes a sum from the sums", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);
  const state1 = advanceState(makeAddUpdates(blocks), makeM5State());

  assert.is(state1.sums.length, 2);

  const state2 = advanceState(makeRollbackUpdates(1), state1);

  assert.is(state2.sums.length, 1);
});

Rollbacks("rollback removes the reverted sum from top sums", async () => {
  const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);

  const state1 = advanceState(makeAddUpdates(blocks), makeM5State());
  const topSums1 = state1.topSumsMap["eth"]["max"];
  const first1 = _.head(topSums1)!;
  assert.is(first1?.end, 13666165);

  const state2 = advanceState(makeRollbackUpdates(1), state1);
  const topSums2 = state2.topSumsMap["eth"]["max"];
  const first2 = _.head(topSums2)!;
  assert.is.not(first2?.end, 13666165);
});

Rollbacks(
  "rollback for 'block' granularity restores the previous fee block",
  async () => {
    const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);

    const state1 = advanceState(makeAddUpdates(blocks));
    const last1 = _.last(state1.feeBlocks)!;
    assert.is(last1.number, blocks[1].number);

    const state2 = advanceState(makeRollbackUpdates(1), state1);
    const last2 = _.last(state2.feeBlocks)!;
    assert.is(last2.number, blocks[0].number);
  },
);

Rollbacks(
  "rollback for timed granularities restore fee blocks within graularity",
  async () => {
    const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);

    const initState = makeM5State();
    const state1 = advanceState(makeAddUpdates(blocks), initState);
    const last1 = _.last(state1.feeBlocks)!;
    assert.is(last1.number, blocks[1].number);

    const state2 = advanceState(makeRollbackUpdates(1), state1);
    const last2 = _.last(state2.feeBlocks)!;
    assert.is(last2.number, blocks[0].number);
  },
);

Rollbacks(
  "rollback consumes a fee block from the feeBlocks rollback buffer",
  async () => {
    const blocks = (await BlocksData.getM5Blocks()).slice(0, 2);
    const finalState = advanceState([
      ...makeAddUpdates(blocks),
      ...makeRollbackUpdates(1),
    ]);

    assert.is(finalState.feeBlockRollbackBuffer.length, 0);
  },
);

Rollbacks("rollback consumes a sum from the sums rollback buffer", async () => {
  const blocks = await BlocksData.getH1Blocks();
  // these 27 blocks set us up exactly so that the last block is the first that pushes several sums out of the 5m time frame and into the rollback buffer.
  const m5Blocks = _.take(blocks, 26);
  const state1 = advanceState(makeAddUpdates(m5Blocks));

  assert.is(state1.sumsRollbackBuffer.length, 4);

  const state2 = advanceState(makeRollbackUpdates(1), state1);

  assert.is(state2.sumsRollbackBuffer.length, 0);
});

Rollbacks(
  "rollback handles multiple rollbacks on block granularity",
  async () => {
    const blocks = (await BlocksData.getM5Blocks()).slice(0, 3);
    const finalState = advanceState([
      ...makeAddUpdates(blocks),
      ...makeRollbackUpdates(2),
    ]);

    assert.is(_.last(finalState.feeBlocks)!.number, blocks[0].number);
  },
);

Rollbacks(
  "rollback handles multiple rollbacks on timed granularity",
  async () => {
    const blocks = (await BlocksData.getM5Blocks()).slice(0, 3);
    const initState = makeM5State();
    const finalState = advanceState(
      [...makeAddUpdates(blocks), ...makeRollbackUpdates(2)],
      initState,
    );

    assert.is(_.last(finalState.feeBlocks)!.number, blocks[0].number);
  },
);

Rollbacks.run();

test.skip("does not advance state for granularities bigger than time frames", async () => {});

test.run();
