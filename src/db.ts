import { camelCase } from "change-case";
import { flow, Lazy } from "fp-ts/lib/function.js";
import * as Ley from "ley";
import postgres, {
  AsRowList,
  PendingQuery,
  Row,
  SerializableParameter,
  TransactionSql,
} from "postgres";
import * as Config from "./config.js";
import { A, O, pipe, T } from "./fp.js";

const config = {
  ssl: "prefer",
  transform: { column: camelCase },
  max: Config.getEnv() === "staging" ? 2 : 6,
  no_prepare: Config.getEnv() === "staging",
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

export const sqlTVoid = flow(
  sqlT,
  T.map(() => undefined),
);

export type SqlArg =
  | typeof sql
  | TransactionSql<{
      bigint: (number: bigint) => string;
    }>;

export const runMigrations = () =>
  Ley.up({
    dir: "migrations",
    config: config,
  });

export const readOptionalFromFirstRow =
  <A>(field: keyof A) =>
  (rows: A[]) =>
    pipe(
      rows,
      A.head,
      O.map((row) => row[field]),
      O.map(O.fromNullable),
      O.flatten,
    );

export const readFromFirstRow =
  <A>(field: keyof A) =>
  (rows: A[]) =>
    pipe(
      rows,
      A.head,
      O.map((row) => row[field]),
    );

export const sqlNotifyT = (channel: string, payload: string) => () =>
  sql.notify(channel, payload);
