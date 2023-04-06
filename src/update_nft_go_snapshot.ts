import * as Db from "./db.js";
import { pipe, T, TE } from "./fp.js";
import * as Log from "./log.js";
import {
  refreshRankedCollections,
  refreshMarketCap,
} from "./nft_go_snapshot.js";

await pipe(
  refreshRankedCollections,
  TE.chain(() => refreshMarketCap),
  TE.match(
    (error) => {
      Log.error("failed to refresh NftGo snapshot", error);
    },
    () => {
      Log.info("refreshed NftGo snapshot");
    },
  ),
  T.chain(() => Db.closeConnection),
)();
