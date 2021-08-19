import postgres from "postgres";
import O from "fp-ts/lib/Option.js";
import { camelCase } from "change-case";
import { pipe } from "fp-ts/lib/function.js";
import Config from "./config.js";

const port = pipe(
  process.env.PGPORT,
  O.fromNullable,
  O.map(Number),
  O.getOrElse(() => 5432),
);

export const sql = postgres({
  host: process.env.PGHOST,
  database: process.env.PGDATABASE,
  password: process.env.PGPASSWORD,
  username: process.env.PGUSER,
  port,
  ssl: Config.env === "staging" ? "require" : "prefer",
  transform: { column: camelCase },
  max: 8,
  types: {
    bigint: postgres.BigInt,
  },
  connection: {
    application_name: Config.name, // Default application_name
  },
});
