import { sql } from "../db.js";
import * as Log from "../log.js";
import * as BurnCategories from "./burn_categories.js";

Log.info("start analyzing burn categories");

// This query is slow. We only want to run one computation at a time with no queueing.
let isUpdating = false;

export const setIsUpdating = (nextIsUpdating: boolean) => {
  isUpdating = nextIsUpdating;
};

sql.listen("blocks-update", () => {
  if (!isUpdating) {
    Log.debug("got blocks update, starting analysis");
    isUpdating = true;
    BurnCategories.updateBurnCategories()();
    return;
  }

  Log.debug("got blocks update, but already analyzing, skipping block");
});
