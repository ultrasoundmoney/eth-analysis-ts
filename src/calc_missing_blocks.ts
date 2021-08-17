import * as BaseFees from "./base_fees.js";
import * as Log from "./log.js";

BaseFees.calcMissingBaseFees().catch((error) => {
  Log.error("error watching and analyzing new blocks", { error });
  throw error;
});
