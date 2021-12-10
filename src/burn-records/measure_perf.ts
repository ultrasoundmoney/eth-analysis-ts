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
// const blocksCount = await sql<{ count: number }[]>`
//   SELECT COUNT(*) FROM blocks
//   WHERE number >= 12965000
//   AND number <= ${lastStoredBlock.number}
// `;

const blocks = await Blocks.getFeeBlocks(12965000, lastStoredBlock.number);

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

setInterval(() => {
  Log.info(`sync missing blocks, eta: ${eta.estimate()}s`);
}, 4000);

for (const block of blocks) {
  for (const recordState of recordStates) {
    BurnRecords.addBlockToState(recordState, block);
  }
  blocksDone = blocksDone + 1;
  eta.report(blocksDone);
}

Performance.logPerf("added all blocks to state in", t0);
