import postgres from "postgres";
import { camelCase } from "change-case";
import Config from "./config.js";

const database = Config.chain === "ropsten" ? "ropsten" : "defaultdb";

export const sql = postgres({
  host: process.env.DB_HOST,
  database,
  password: process.env.DB_PASSWORD,
  username: process.env.DB_USER,
  transform: { column: camelCase },
});
