import { sql } from "../db.js";
import { O, OAlt, pipe, T } from "../fp.js";
import * as Log from "../log.js";
import * as BurnCategories from "./burn_categories.js";

Log.info("start analyzing burn categories");

const updateForBlockNumber = (blockNumber: number): T.Task<void> =>
  pipe(
    Log.debugT(`updating burn categories for block number: ${blockNumber}`),
    T.chainIOK(() => () => {
      BurnCategories.setIsUpdating(true);
    }),
    T.chain(() => BurnCategories.updateBurnCategories()),
    T.chainIOK(() => () => {
      BurnCategories.setLastUpdated(blockNumber);
    }),
  );

const updateWithThrottle = (blockNumber: number): T.Task<void> =>
  pipe(
    BurnCategories.getLastUpdated(),
    O.match(
      // Never updated before.
      () => updateForBlockNumber(blockNumber),
      (lastUpdated) =>
        blockNumber - lastUpdated < MIN_BLOCKS_UNTIL_NEXT_UPDATE
          ? // Updated recentely, skip.
            Log.debugT("updated burn categories < 50 blocks ago, skipping")
          : // Updated at least 10 min ago, update now.
            updateForBlockNumber(blockNumber),
    ),
  );

// Roughly 1 day when no slots are missed (12s per block).
const MIN_BLOCKS_UNTIL_NEXT_UPDATE = 7200;

sql.listen("blocks-update", (update) => {
  pipe(
    update,
    O.fromNullable,
    O.map((str) => JSON.parse(str) as { number: number }),
    OAlt.getOrThrow("expect blocks update to contain parseable block number"),
    ({ number: blockNumber }) =>
      BurnCategories.getIsUpdating()
        ? Log.debugT(
            "got blocks update, but already updating categories, skipping update",
          )
        : updateWithThrottle(blockNumber),
  )();
});
