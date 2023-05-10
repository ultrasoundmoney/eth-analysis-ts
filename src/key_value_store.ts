import * as Db from "./db.js";
import { O, pipe, T, TO, TOAlt } from "./fp.js";

export const getValue = <A>(key: string): TO.TaskOption<A> =>
  pipe(
    Db.sqlT<{ value: A }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${key}
    `,
    T.map(O.fromNullableK((rows) => rows[0]?.value)),
  );

export const getValueStr = (key: string): TO.TaskOption<string> =>
  pipe(
    Db.sqlT<{ value: string }[]>`
      SELECT value::text FROM key_value_store
      WHERE key = ${key}
    `,
    T.map(O.fromNullableK((rows) => rows[0]?.value)),
  );

export const getValueUnsafe = <A>(key: string): T.Task<A> =>
  pipe(
    getValue<A>(key),
    TOAlt.expect(`expected a value to exist for key ${key}`),
  );

export const storeValue = (key: string, value: unknown): T.Task<void> =>
  Db.sqlTVoid`
    INSERT INTO key_value_store
      ${Db.values({
        key,
        value: JSON.stringify(value),
      })}
    ON CONFLICT (key) DO UPDATE SET
      value = excluded.value
  `;
