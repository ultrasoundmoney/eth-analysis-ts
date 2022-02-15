import * as Db from "./db.js";
import * as Log from "./log.js";
import * as PeRatios from "./pe_ratios.js";

Log.info("updating PE ratios");
await PeRatios.updatePeRatios();
Log.info("done storing PE ratios");
await Db.closeConnection();
