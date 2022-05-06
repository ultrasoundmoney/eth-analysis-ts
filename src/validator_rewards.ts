import * as DateFns from "date-fns";
import { getLastStateWithBlock } from "./beacon_states.js";
import * as Db from "./db.js";
import { flow, OAlt, pipe, T, TOAlt } from "./fp.js";
import * as Log from "./log.js";
import { genesisTimestamp } from "./validator_balances.js";

const genisisStateRoot =
  "0x7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b";

export const getInitialDeposits = () =>
  pipe(
    Db.sqlT<{ validatorBalanceSum: string }[]>`
      SELECT validator_balance_sum FROM beacon_states
      WHERE state_root = ${genisisStateRoot}
    `,
    T.map(
      flow(
        Db.readFromFirstRow("validatorBalanceSum"),
        OAlt.getOrThrow("failed to get genesis validator balance sum"),
        BigInt,
      ),
    ),
  );

export const getGweiIssued = () =>
  pipe(
    T.Do,
    T.apS("initialDeposits", getInitialDeposits()),
    T.apS(
      "lastState",
      pipe(
        getLastStateWithBlock(),
        TOAlt.getOrThrow("failed to get last beacon state"),
      ),
    ),
    T.map(
      ({ initialDeposits, lastState }) =>
        lastState.validatorBalanceSum -
        lastState.depositSumAggregated -
        initialDeposits,
    ),
  );

const getTipsPerYear = () =>
  pipe(
    Db.sqlT<{ tipsPerYear: number }[]>`
      SELECT SUM(tips) AS tips_per_year FROM blocks
      WHERE mined_at >= NOW() - '1 year'::interval
    `,
    T.map(
      flow(
        Db.readFromFirstRow("tipsPerYear"),
        OAlt.getOrThrow("failed to get tips per year"),
      ),
    ),
  );

export const getValidatorRewards = () =>
  pipe(
    T.Do,
    T.apS("gweiIssued", getGweiIssued()),
    T.apS(
      "validatorBalanceSum",
      pipe(
        getLastStateWithBlock(),
        TOAlt.getOrThrow("failed to get last beacon state with block"),
        T.map((state) => state.validatorBalanceSum),
      ),
    ),
    T.apS("tipsPerYear", getTipsPerYear()),
    T.map(({ gweiIssued, validatorBalanceSum, tipsPerYear }) => {
      const daysSinceGenesis = pipe(genesisTimestamp, (dt) =>
        DateFns.differenceInDays(new Date(), dt),
      );
      const validatorGwei = 32 * 1e9;
      const validatorShare = validatorGwei / Number(validatorBalanceSum);

      const gweiIssuedPerDay = Number(gweiIssued) / daysSinceGenesis;
      const gweiIssuedPerYear = gweiIssuedPerDay * 365;
      const gweiEarnedPerYear = gweiIssuedPerYear * validatorShare;
      const issuanceApr = gweiEarnedPerYear / validatorGwei;

      const tipsEarnedPerYear = (tipsPerYear / 1e9) * validatorShare;
      const tipsApr = tipsEarnedPerYear / validatorGwei;

      Log.debug("ETH issued", gweiIssued / 1_000_000_000n);
      Log.debug(
        "ETH validator balance sum",
        validatorBalanceSum / 1_000_000_000n,
      );
      Log.debug("days since genesis", daysSinceGenesis);
      Log.debug("gweiIssuedPerDay", gweiIssuedPerDay);
      Log.debug("gweiIssuedPerYear", gweiIssuedPerYear);
      Log.debug("gweiEarnedPerYear", gweiEarnedPerYear);
      Log.debug("validator share", validatorShare);
      Log.debug("Issuance APR", issuanceApr);

      Log.debug("tipsPerYear", tipsPerYear);
      Log.debug("tipsEarnedPerYear", tipsEarnedPerYear);
      Log.debug("tips APR", tipsApr);

      return {
        issuance: {
          annualReward: gweiEarnedPerYear,
          apr: issuanceApr,
        },
        tips: {
          annualReward: tipsEarnedPerYear,
          apr: tipsApr,
        },
        mev: {
          annualReward: 0.3 * 1e9,
          apr: (0.3 * 1e9) / validatorGwei,
        },
      };
    }),
  );
