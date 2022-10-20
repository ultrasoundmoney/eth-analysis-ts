import * as BlocksSync from "../blocks/sync.js";
import * as Log from "../log.js";
import * as Db from "../db.js";
import * as ExecutionNode from "../execution_node.js";

await BlocksSync.syncBlock(15116564);
await ExecutionNode.closeConnections();
await Db.closeConnection();
Log.info("done");
