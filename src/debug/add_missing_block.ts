import * as Log from "../log.js";
import * as BlocksSync from "../blocks/sync.js";
import * as EthNode from "../eth_node.js";

await EthNode.connect();
await BlocksSync.syncBlock(13267853);
Log.info("done");
