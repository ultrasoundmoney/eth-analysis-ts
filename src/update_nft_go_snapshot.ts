import { pipe, T, TE } from "./fp.js";
import * as Db from "./db.js";
import * as Log from "./log.js";
import {
  refreshRankedCollections,
  refreshMarketCap,
} from "./nft_go_snapshot.js";

await pipe(
  refreshRankedCollections(),
  TE.chainW(() => refreshMarketCap()),
  TE.matchE(
    (e) => {
      Log.error("failed to fresh NftGo snapshot", e);
      return T.of(undefined);
    },
    () => {
      Log.info("refreshed NftGo snapshot");
      return () => Db.closeConnection();
    },
  ),
)();
