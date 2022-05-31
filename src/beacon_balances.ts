import * as Db from "./db.js";
import { pipe } from "./fp.js";

export const getValidatorBalancesByDay = () =>
  pipe(
    Db.sqlT<{ timestamp: Date; gwei: string }[]>`
      SELECT timestamp, gwei FROM eth_in_validators
    `,
  );
