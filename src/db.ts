import { camelCase } from "change-case";
import { Lazy, pipe } from "fp-ts/lib/function.js";
import O from "fp-ts/lib/Option.js";
import postgres, {
  AsRowList,
  PendingQuery,
  Row,
  SerializableParameter,
  TransactionSql,
} from "postgres";
import * as Config from "./config.js";

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
  ssl: "prefer",
  transform: { column: camelCase },
  max: Config.getEnv() === "staging" ? 2 : 6,
  no_prepare: Config.getEnv() === "staging",
  types: {
    bigint: postgres.BigInt,
  },
  connection: {
    application_name: Config.getName(),
  },
});

/* eslint-disable @typescript-eslint/no-explicit-any */
export const sqlT =
  <A extends any[] = Row[]>(
    template: TemplateStringsArray,
    ...args: SerializableParameter[]
  ): Lazy<PendingQuery<AsRowList<A>>> =>
  () =>
    sql(template, ...args);
/* eslint-enable @typescript-eslint/no-explicit-any */

export type SqlArg =
  | typeof sql
  | TransactionSql<{
      bigint: (number: bigint) => string;
    }>;
