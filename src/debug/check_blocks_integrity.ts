import makeEta from "simple-eta";
import { sql } from "../db.js";
import * as EthNode from "../eth_node.js";
import { hexFromNumber } from "../hexadecimal.js";
import * as Log from "../log.js";

const left = await sql`SELECT MIN(number) FROM blocks`.then(
  (rows) => rows[0].min,
);
const right = await sql`SELECT MAX(number) FROM blocks`.then(
  (rows) => rows[0].max,
);
const blocksToScan = right - left + 1;
let blocksDone = 0;
const eta = makeEta({ max: blocksToScan });

await sql<
  { hash: string; number: number; baseFeePerGas: string; gasUsed: string }[]
>`
  SELECT hash, number, base_fee_per_gas, gas_used FROM blocks
  ORDER By number ASC
`.cursor(1000, async (rows) => {
  for (const row of rows) {
    const block = await EthNode.getBlock(row.number);
    if (row.hash !== block?.hash) {
      throw new Error(
        `found bad block ${row.number}, our hash: ${row.hash}, their hash: ${block?.hash}`,
      );
    }

    if (
      hexFromNumber(Number(row.baseFeePerGas)) !== block.baseFeePerGas ||
      hexFromNumber(Number(row.gasUsed)) !== block.gasUsed
    ) {
      Log.debug("found bad block", {
        row,
        block: {
          ...block,
          transactions: null,
          baseFeePerGasNum: Number(block.baseFeePerGas),
          gasUsedNum: Number(block.gasUsed),
        },
      });
      throw new Error("hit bad block");
    }
  }

  blocksDone = blocksDone + 1000;
  eta.report(blocksDone);
  eta.estimate();
  Log.debug(
    `blocks scanned: ${blocksDone}, eta: ${eta.estimate().toFixed(0)}s`,
  );
});
