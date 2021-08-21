import * as BaseFees from "./base_fees.js";
import { setName } from "./config.js";
import * as Log from "./log.js";

setName("analyze-missing-blocks");

BaseFees.analyzeMissingBlocks().catch((error) => {
  Log.error("error watching and analyzing new blocks", { error });
  throw error;
});
