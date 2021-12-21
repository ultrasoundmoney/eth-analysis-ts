import { camelCase } from "change-case";
import { Lazy } from "fp-ts/lib/function";
import * as Ley from "ley";
import postgres, {
  AsRowList,
  PendingQuery,
  Row,
  SerializableParameter,
  TransactionSql,
} from "postgres";
import * as Config from "./config.js";

const config = {
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
} as const;

export const sql = postgres(config);

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

await Ley.up({
  dir: "migrations",
  config: config,
});
