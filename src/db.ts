import postgres from "postgres";
import { camelCase } from "change-case";

export const sql = postgres({
  host: process.env.DB_HOST,
  // database: process.env.DB_DATABASE,
  database: "ropsten",
  password: process.env.DB_PASSWORD,
  username: process.env.DB_USER,
  transform: { column: camelCase },
});
