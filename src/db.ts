import { camelCase } from "change-case";
import Ley from "ley";
import postgres, {
  AsRowList,
  PendingQuery,
  Row,
  SerializableParameter,
} from "postgres";
import * as Config from "./config.js";
import { A, flow, O, pipe, T } from "./fp.js";

const connectionsPerServiceProd: Partial<Record<string, number>> = {
  "analyze-blocks": 8,
};

const getMax = (env: Config.Env, name: string | undefined) =>
  pipe(
    env === "prod" && name !== undefined ? O.some(name) : O.none,
    // Names look something like: analyze-blocks-f49657576-shbc6
    O.chain(
      O.fromNullableK((name) =>
        Object.keys(connectionsPerServiceProd).find((key) =>
          name.startsWith(key),
        ),
      ),
    ),
    O.chain(O.fromNullableK((key) => connectionsPerServiceProd[key])),
    O.getOrElse(() => 2),
  );

const config = {
  ssl: "prefer",
  // Consider removing manual conversions in favor of v3 automatic.
  // transform: { column: { to: postgres.fromCamel, from: postgres.toCamel } },
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
  ): (() => PendingQuery<AsRowList<A>>) =>
  () =>
    sql(template, ...args);
/* eslint-enable @typescript-eslint/no-explicit-any */

export const sqlTVoid = flow(
  sqlT,
  T.map((): void => undefined),
);

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

export const sqlTNotify = (channel: string, payload: string) =>
  pipe(
    () => sql.notify(channel, payload),
    T.map((): void => undefined),
  );

export const runMigrations = () =>
  Ley.up({
    dir: "migrations",
    config: config,
  });

export const closeConnection = () => sql.end();

export const query = sql;

export const values = sql;

export const array = sql.array;
