import makeEta from "simple-eta";
import * as Blocks from "../blocks/blocks.js";
import * as Db from "../db.js";
import * as ExecutionNode from "../execution_node.js";
import { pipe, TOAlt } from "../fp.js";
import * as Log from "../log.js";

const left = await Db.sql`SELECT MIN(number) FROM blocks`.then(
  (rows) => rows[0].min,
);
const right = await Db.sql`SELECT MAX(number) FROM blocks`.then(
  (rows) => rows[0].max,
);
const blocksToScan = right - left + 1;
let blocksDone = 0;
const eta = makeEta({ max: blocksToScan });

await Db.sql<
  { hash: string; number: number; baseFeePerGas: number; gasUsed: number }[]
>`
  SELECT hash, number, base_fee_per_gas::float8, gas_used::float8 FROM blocks
  ORDER BY number ASC
`.cursor(1000, async (rows) => {
  for (const row of rows) {
    const block = await pipe(
      Blocks.getBlockSafe(row.number),
      TOAlt.getOrThrow(`failed to get block ${row.number} from node`),
    )();
    if (row.hash !== block.hash) {
      throw new Error(
        `found bad block ${row.number}, our hash: ${row.hash}, their hash: ${block?.hash}`,
      );
    }

    if (
      row.baseFeePerGas !== block.baseFeePerGas ||
      row.gasUsed !== block.gasUsed
    ) {
      Log.debug("found bad block", {
        stored: row,
        onChain: {
          number: Number(block.number),
          hash: block.hash,
          baseFeePerGas: Number(block.baseFeePerGas),
          gasUsed: Number(block.gasUsed),
        },
      });
      throw new Error("hit bad block");
    }
  }

  blocksDone = blocksDone + 1000;
  eta.report(blocksDone);
  Log.debug(
    `blocks scanned: ${blocksDone}, eta: ${eta.estimate().toFixed(0)}s`,
  );
});

await ExecutionNode.closeConnections();
await Db.closeConnection();
