import * as Db from "./db.js";
import { flow, O, pipe, T, TO } from "./fp.js";

export const EFFECTIVE_BALANCE_SUM_CACHE_KEY = "effective-balance-sum";

export const getLastEffectiveBalanceSum = (): TO.TaskOption<number> =>
  pipe(
    Db.sqlT<{ effectiveBalanceSum: number }[]>`
      SELECT effective_balance_sum FROM beacon_states
      WHERE effective_balance_sum IS NOT NULL
      ORDER BY slot DESC
      LIMIT 1
    `,
    T.map(flow((rows) => rows[0]?.effectiveBalanceSum, O.fromNullable)),
  );
