import * as BeaconNode from "./beacon_node.js";
import { getLastStateWithBlock } from "./beacon_states.js";
import * as BeaconTime from "./beacon_time.js";
import * as Db from "./db.js";
import { A, flow, O, OAlt, pipe, T, TE, TEAlt } from "./fp.js";
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

// In Gwei
// Obtained by getting validator balances for slot 0, which had zero deposits.
export const getInitialDeposits = () => 674144000000000n;

export const getLastDepositSumAggregated = () =>
  pipe(
    Db.sqlT<{ depositSumAggregated: string }[]>`
      SELECT deposit_sum_aggregated FROM beacon_states
      ORDER BY slot DESC
      LIMIT 1
    `,
    T.map(
      flow(
        Db.readFromFirstRow("depositSumAggregated"),
        O.map(BigInt),
        OAlt.getOrThrow(
          "failed to get last deposit_sum_aggregated, empty table",
        ),
      ),
    ),
  );

export const getEffectiveBalanceSum = (stateRoot: string) =>
  pipe(
    BeaconNode.getValidatorsByState(stateRoot),
    TE.map(
      flow(
        A.map(
          (validatorEnvelope) => validatorEnvelope.validator.effective_balance,
        ),
        A.reduce(0n, (sum, balance) => sum + balance),
      ),
    ),
  );

export const getLastEffectiveBalanceSum = () =>
  pipe(
    getLastStateWithBlock(),
    TE.fromTaskOption(
      () => new Error("failed to get last state with block, empty table"),
    ),
    TE.chainW((lastState) => getEffectiveBalanceSum(lastState.stateRoot)),
  );
