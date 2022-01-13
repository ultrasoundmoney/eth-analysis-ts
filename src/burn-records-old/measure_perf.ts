import * as DateFns from "date-fns";
import { readFileSync } from "fs";
import _ from "lodash";
import makeEta from "simple-eta";
import * as Blocks from "../blocks/blocks.js";
import { deserializeBigInt } from "../json.js";
import * as Log from "../log.js";
import * as Performance from "../performance.js";
import * as BurnRecords from "./burn_records.js";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(BigInt.prototype as any).toJSON = function () {
  return this.toString();
};

Log.info("measuring add all blocks performance");

const t0 = performance.now();
const lastStoredBlock = await Blocks.getLastStoredBlock()();
Log.info(`last stored block is: ${lastStoredBlock.number}`);

// const blocks = await Blocks.getFeeBlocks(
//   Blocks.londonHardForkBlockNumber,
//   lastStoredBlock.number,
// );
// writeFileSync("./blocks-all.json", JSON.stringify(blocks, serialize));
const blocks = JSON.parse(
  readFileSync("./blocks-all.json", "utf8"),
  deserializeBigInt,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
).map((block: any) => ({ ...block, minedAt: DateFns.parseISO(block.minedAt) }));
Log.info(`${blocks.length} blocks total`);

Performance.logPerf("get all blocks", t0);

export const recordStates = BurnRecords.granularities.map((granularity) =>
  BurnRecords.makeRecordState(granularity, "all"),
);

let blocksDone = 0;
const eta = makeEta({
  max: blocks.length,
});

const logPerf = _.throttle((block) => {
  Log.info(
    `burn records process all eta estimate: ${eta.estimate()}s, last block: ${
      block.number
    }`,
  );
}, 2000);

for (const block of blocks) {
  for (const recordState of recordStates) {
    BurnRecords.addBlockToState(recordState, block);
  }
  blocksDone = blocksDone + 1;
  eta.report(blocksDone);
  if (new Date().getSeconds() % 8 === 0) {
    logPerf(block);
  }
}

Performance.logPerf("analyse all blocks", t0);
