import * as BlocksSync from "../blocks/sync.js";
import * as Log from "../log.js";
import * as Db from "../db.js";
import * as ExecutionNode from "../execution_node.js";

await BlocksSync.syncBlock(600000, true);
await ExecutionNode.closeConnections();
await Db.closeConnection();
Log.info("done");
