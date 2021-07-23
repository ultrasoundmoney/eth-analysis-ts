import * as Blocks from "./blocks";
import { sql } from "./db";
import { delay } from "./delay";
import * as Log from "./log";

(async () => {
  await Blocks.syncBlocks();

  // Write after end errors without this
  await delay(1000);

  await sql.end();

  process.exit();
})().catch((error) => {
  Log.error("main error", error);
  process.exit(1);
});
