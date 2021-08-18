import postgres from "postgres";
import { camelCase } from "change-case";

export const sql = postgres({
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  password: process.env.PGPASSWORD,
  username: process.env.PGUSER,
  transform: { column: camelCase },
  max: 8,
  types: {
    bigint: postgres.BigInt,
  },
});
