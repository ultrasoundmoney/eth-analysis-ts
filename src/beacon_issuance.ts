import {
  getEthInValidatorsByDay,
  getInitialDeposits,
} from "./beacon_balances.js";
import { BeaconStateWithBlock } from "./beacon_states.js";
import * as BeaconTime from "./beacon_time.js";
import * as Db from "./db.js";
import { A, flow, O, OAlt, pipe, T, TAlt, TOAlt } from "./fp.js";

const storeIssuance = (timestamp: Date, issuance: bigint) => Db.sqlTVoid`
  INSERT INTO beacon_issuance
    ${Db.values({
      timestamp,
      issuance,
    })}
`;

export const onAddStateWithBlock = (state: BeaconStateWithBlock) =>
  TAlt.when(
    BeaconTime.getIsFirstOfDaySlot(state.slot),
    pipe(
      T.Do,
      T.apS("timestamp", T.of(BeaconTime.getStartOfDayFromSlot(state.slot))),
      T.bind("validatorBalanceSum", ({ timestamp }) =>
        pipe(
          timestamp,
          getEthInValidatorsByDay,
          TOAlt.getOrThrow(
            `failed to get validator balance sum for ${timestamp}`,
          ),
        ),
      ),
      T.chain(({ timestamp, validatorBalanceSum }) =>
        storeIssuance(
          timestamp,
          validatorBalanceSum -
            state.depositSumAggregated -
            getInitialDeposits(),
        ),
      ),
    ),
  );

export const getIssuanceByDay = () =>
  Db.sqlT<{ timestamp: Date; issuance: string }[]>`
    SELECT timestamp, issuance FROM beacon_issuance
  `;

export const getLastIssuancePerDay = () =>
  pipe(
    Db.sqlT<{ issuance: string }[]>`
      SELECT issuance FROM beacon_issuance
      ORDER BY timestamp DESC
      LIMIT 2
    `,
    T.map(
      flow(
        (rows) => OAlt.seqT(A.lookup(0)(rows), A.lookup(1)(rows)),
        O.map(
          ([issuanceDayN, issuanceDayNMinOne]) =>
            BigInt(issuanceDayN.issuance) - BigInt(issuanceDayNMinOne.issuance),
        ),
      ),
    ),
  );
