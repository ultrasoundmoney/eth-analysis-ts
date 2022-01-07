import * as DateFns from "date-fns";
import { setInterval } from "timers/promises";
import * as DateFnsAlt from "../date_fns_alt.js";
import { sql, sqlT } from "../db.js";
import * as DefiPulse from "../defi_pulse.js";
import * as Duration from "../duration.js";
import { B, flow, O, pipe, T, TE, TO } from "../fp.js";

type LastEthLocked = {
  timestamp: Date;
  ethLocked: number;
};

const intervalIterator = setInterval(Duration.millisFromHours(1), Date.now());

const ethLockedKey = "eth-locked";

export const getLastEthLocked = () =>
  pipe(
    sqlT<{ value: { timestamp: number; ethLocked: number } }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${ethLockedKey}
    `,
    T.map(
      flow(
        (rows) => rows[0],
        O.fromNullable,
        O.map((row) => ({
          timestamp: DateFns.fromUnixTime(row.value.timestamp),
          ethLocked: row.value.ethLocked,
        })),
      ),
    ),
  );

const storeEthLocked = (ethLocked: number) =>
  sqlT`
    INSERT INTO key_value_store (
      key,
      value
    ) VALUES (
      ${ethLockedKey},
      ${sql.json({
        timestamp: DateFns.getUnixTime(new Date()),
        ethLocked,
      })}
    )
    ON CONFLICT (key) DO UPDATE SET
      value = excluded.value
    WHERE
      key_value_store.key = ${ethLockedKey}
  `;

const updateEthLocked = () =>
  pipe(
    DefiPulse.getEthLocked(),
    TE.chainTaskK(storeEthLocked),
    TE.map(() => undefined),
  );

const maxAge = Duration.millisFromDays(2);

const getIsEthLockedFresh = (lastEthLocked: LastEthLocked) =>
  DateFnsAlt.millisecondsBetweenAbs(lastEthLocked.timestamp, new Date()) <=
  maxAge;

const refreshEthLocked = () =>
  pipe(
    getLastEthLocked(),
    TO.matchE(
      () => updateEthLocked(),
      (lastStored) =>
        pipe(
          getIsEthLockedFresh(lastStored),
          B.match(
            () => updateEthLocked(),
            () => TE.of(undefined),
          ),
        ),
    ),
  );

const continuouslyUpdate = async () => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of intervalIterator) {
    await refreshEthLocked()();
  }
};

export const init = () =>
  pipe(
    refreshEthLocked(),
    TE.chainFirstIOK(() => () => {
      continuouslyUpdate();
    }),
  );
