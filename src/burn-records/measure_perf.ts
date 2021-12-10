import _ from "lodash";
import makeEta from "simple-eta";
import * as Blocks from "../blocks/blocks.js";
import * as Cartesian from "../cartesian.js";
// import { sql } from "../db.js";
import { denominations } from "../denominations.js";
import * as Log from "../log.js";
import * as Performance from "../performance.js";
import * as BurnRecords from "./burn_records.js";

Log.info("measuring add all blocks performance");

const t0 = performance.now();
const lastStoredBlock = await Blocks.getLastStoredBlock();
Log.info(`last stored block is: ${lastStoredBlock.number}`);
// const blocksCount = await sql<{ count: number }[]>`
//   SELECT COUNT(*) FROM blocks
//   WHERE number >= 12965000
//   AND number <= ${lastStoredBlock.number}
// `;

const blocks = await Blocks.getFeeBlocks(12965000, lastStoredBlock.number);
// const blocks = await Blocks.getFeeBlocks(12965000, 13065000);

Performance.logPerf("fetched all blocks in", t0);

export const recordStates = Cartesian.make3(
  denominations,
  BurnRecords.granularities,
  BurnRecords.sortings,
).map(([denomination, granularity, sorting]) =>
  BurnRecords.makeRecordState(denomination, granularity, sorting, "all"),
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
