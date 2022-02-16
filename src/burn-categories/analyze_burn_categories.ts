import { sql } from "../db.js";
import * as Log from "../log.js";
import * as BurnCategories from "./burn_categories.js";

Log.info("start analyzing burn categories");

sql.listen("blocks-update", () => {
  if (!BurnCategories.getIsUpdating()) {
    Log.debug("got blocks update, starting analysis");
    BurnCategories.setIsUpdating(true);
    BurnCategories.updateBurnCategories()();
    return;
  }

  Log.debug("got blocks update, but already analyzing, skipping block");
});
