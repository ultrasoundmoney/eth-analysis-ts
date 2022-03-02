import * as BlocksSync from "../blocks/sync.js";
import * as Log from "../log.js";

await BlocksSync.syncBlock(13267853);
Log.info("done");
