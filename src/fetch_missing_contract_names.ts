import * as Log from "./log.js";
import * as Contracts from "./contracts.js";
import { sql } from "./db.js";

Contracts.fetchMissingContractNames()
  .then(async () => {
    Log.info("> done analyzing gas");
    await sql.end();
  })
  .catch((error) => {
    Log.error("> error analyzing gas", { error });
    throw error;
  });
