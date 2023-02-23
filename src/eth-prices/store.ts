import * as Db from "../db.js";
import { A, flow, O, pipe, T, TE } from "../fp.js";
import { EthPrice, MissingPriceError } from "./index.js";

export const priceByMinute = (
  roundMinute: Date,
): TE.TaskEither<MissingPriceError, EthPrice> =>
  pipe(
    Db.sqlT<EthPrice[]>`
      SELECT
        timestamp,
        ethusd
      FROM eth_prices
      WHERE timestamp = ${roundMinute}
      LIMIT 1
    `,
    T.map(flow(A.head, O.chain(O.fromNullable))),
    TE.fromTaskOption(
      () =>
        new MissingPriceError(
          `no eth price found for minute ${roundMinute.toISOString()}`,
        ),
    ),
  );

export const closestPrice = (
  timestamp: Date,
): TE.TaskEither<MissingPriceError, EthPrice> =>
  pipe(
    Db.sqlT<EthPrice[]>`
      SELECT
        timestamp,
        ethusd
      FROM eth_prices
      ORDER BY ABS(EXTRACT(epoch FROM (timestamp - ${timestamp}::timestamp )))
      LIMIT 1
    `,
    T.map(flow(A.head, O.chain(O.fromNullable))),
    TE.fromTaskOption(
      () =>
        new MissingPriceError(
          `no eth price found for ${timestamp.toISOString()}`,
        ),
    ),
  );
