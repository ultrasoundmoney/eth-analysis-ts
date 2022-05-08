import * as BeaconRewards from "./beacon_rewards.js";
import * as Db from "./db.js";
import { pipe, TE } from "./fp.js";
import * as Log from "./log.js";

await pipe(
  BeaconRewards.updateValidatorRewards(),
  TE.match(
    (e) => Log.alert("failed to update validator rewards", e),
    () => Log.info("successfully updated validator rewards"),
  ),
)();

await Db.closeConnection();
