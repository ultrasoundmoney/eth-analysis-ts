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

const connectionsPerServiceProd: Partial<Record<string, number>> = {
  "analyze-blocks": 8,
};

const getMax = (env: Config.Env, name: string | undefined) =>
  env !== "prod" || name === undefined
    ? 2
    : connectionsPerServiceProd[name] ?? 2;

const config = {
  ssl: "prefer",
  transform: { column: camelCase },
  max: getMax(Config.getEnv(), Config.getName()),
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
  T.map((): void => undefined),
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

export const sqlTNotify = (channel: string, payload: string) => () =>
  sql.notify(channel, payload);

export const closeConnection = () => sql.end();

export const query = sql;

export const values = sql;
