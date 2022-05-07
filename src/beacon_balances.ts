import * as DateFns from "date-fns";
import * as BeaconNode from "./beacon_node.js";
import * as BeaconTime from "./beacon_time.js";
import * as Db from "./db.js";
import { ethFromGwei } from "./eth_units.js";
import { A, flow, O, pipe, T, TE, TEAlt } from "./fp.js";
import * as Log from "./log.js";
import { measureTaskPerf } from "./performance.js";

export const storeValidatorSumForDay = (
  slot: number,
  validatorBalanceSum: bigint,
) =>
  pipe(
    BeaconTime.getStartOfDayFromSlot(slot),
    T.of,
    T.chainFirstIOK((timestamp) =>
      Log.debugIO(
        `storing validator sum for day, slot: ${slot}, timestamp: ${timestamp.toISOString()}, gwei: ${validatorBalanceSum}`,
      ),
    ),
    T.chain(
      (timestamp) =>
        Db.sqlTVoid`
          INSERT INTO eth_in_validators
            ${Db.values({
              timestamp: timestamp,
              gwei: validatorBalanceSum,
            })}
          ON CONFLICT (timestamp) DO NOTHING
        `,
    ),
  );

export type EthInValidators = { t: number; v: number }[];

export const getValidatorBalancesByDay = () =>
  pipe(
    Db.sqlT<{ timestamp: Date; gwei: string }[]>`
      SELECT timestamp, gwei FROM eth_in_validators
    `,
  );

const sumValidatorBalances = (
  validatorBalances: BeaconNode.ValidatorBalance[],
) =>
  pipe(
    validatorBalances,
    A.reduce(0n, (sum, validatorBalance) => sum + validatorBalance.balance),
  );

export const onSyncSlot = (slot: number, stateRoot: string) =>
  TEAlt.when(
    BeaconTime.getIsFirstOfDaySlot(slot),
    pipe(
      BeaconNode.getValidatorBalances(stateRoot),
      (task) => measureTaskPerf("get validator balances", task),
      TE.map(sumValidatorBalances),
      TE.chainTaskK((sum) => storeValidatorSumForDay(slot, sum)),
    ),
  );

export const getEthInValidatorsByDay = (dt: Date) =>
  pipe(
    Db.sqlT<{ gwei: string }[]>`
      SELECT gwei FROM eth_in_validators
      WHERE timestamp = ${dt}
    `,
    T.map(flow(Db.readFromFirstRow("gwei"), O.map(BigInt))),
  );
