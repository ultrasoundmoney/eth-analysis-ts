import * as DateFns from "date-fns";
import { getEthInValidatorsByDay } from "./beacon_balances.js";
import { getInitialDeposits } from "./beacon_rewards.js";
import { BeaconStateWithBlock } from "./beacon_states.js";
import * as BeaconTime from "./beacon_time.js";
import * as Db from "./db.js";
import { ethFromGwei } from "./eth_units.js";
import { A, pipe, T, TAlt, TOAlt } from "./fp.js";

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
      T.apS(
        "initialDeposits",
        pipe(
          getInitialDeposits(),
          TOAlt.getOrThrow("expected initial deposits to be stored"),
        ),
      ),
      T.chain(({ timestamp, validatorBalanceSum, initialDeposits }) =>
        storeIssuance(
          timestamp,
          validatorBalanceSum - state.depositSumAggregated - initialDeposits,
        ),
      ),
    ),
  );

export const getIssuanceByDay = () =>
  Db.sqlT<{ timestamp: Date; issuance: string }[]>`
    SELECT timestamp, issuance FROM beacon_issuance
  `;
