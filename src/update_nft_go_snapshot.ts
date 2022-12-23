import * as Db from "./db.js";
import * as Log from "./log.js";
import {
  refreshRankedCollections,
  refreshMarketCap,
} from "./nft_go_snapshot.js";

try {
  await refreshRankedCollections();
  await refreshMarketCap();
  Log.info("refreshed NftGo snapshot");
} catch (error) {
  Log.error("failed to fresh NftGo snapshot", error);
}
await Db.closeConnection();
