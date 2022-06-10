import { camelCaseKeys } from "./casing.js";
import * as Db from "./db.js";
import { flow, O, pipe, T, TOAlt } from "./fp.js";

export const getValue = <A>(key: string) =>
  pipe(
    Db.sqlT<{ value: A }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${key}
    `,
    T.map(
      flow(
        O.fromNullableK((rows) => rows[0]?.value),
        O.map(camelCaseKeys),
      ),
    ),
  );

export const getValueUnsafe = <A>(key: string) =>
  pipe(
    getValue<A>(key),
    TOAlt.getOrThrow(`expected a value to exist for key ${key}`),
  );

export const storeValue = (key: string, value: unknown) =>
  Db.sqlTVoid`
    INSERT INTO key_value_store
      ${Db.values({
        key,
        value: JSON.stringify(value),
      })}
    ON CONFLICT (key) DO UPDATE SET
      value = excluded.value
  `;
