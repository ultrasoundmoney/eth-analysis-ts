import * as BeaconBalances from "./beacon_balances.js";
import * as BeaconTime from "./beacon_time.js";
import * as Db from "./db.js";
import { ethFromGwei, gweiFromEth, gweiFromWei } from "./eth_units.js";
import { flow, OAlt, pipe, T, TE } from "./fp.js";
import * as KeyValueStore from "./key_value_store.js";
import * as Log from "./log.js";

// const genisisStateRoot =
//   "0x7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b";

// export const getInitialDeposits = () =>
//   pipe(
//     Db.sqlT<{ validatorBalanceSum: string }[]>`
//       SELECT validator_balance_sum FROM eth_in_validators
//       WHERE state_root = ${genisisStateRoot}
//     `,
//     T.map(
//       flow(
//         Db.readFromFirstRow("validatorBalanceSum"),
//         OAlt.getOrThrow("failed to get genesis validator balance sum"),
//         BigInt,
//       ),
//     ),
//   );

// In wei
const getTipsSinceGenesis = () =>
  pipe(
    Db.sqlT<{ tipsSinceGenesis: number }[]>`
      SELECT SUM(tips) AS tips_since_genesis FROM blocks
      WHERE mined_at >= ${BeaconTime.genesisTimestamp}
    `,
    T.map(
      flow(
        Db.readFromFirstRow("tipsSinceGenesis"),
        OAlt.getOrThrow("failed to get tips since genesis"),
      ),
    ),
  );

const getMaxIssuance = (totalEffectiveBalanceGwei: bigint) => {
  const GWEI_PER_ETH = 10 ** 9;

  const totalEffectiveBalance =
    Number(totalEffectiveBalanceGwei) / GWEI_PER_ETH;

  // Number of active validators
  const ACTIVE_VALIDATORS = totalEffectiveBalance / 32;

  // Balance at stake (in Gwei)
  const MAX_EFFECTIVE_BALANCE = 32 * GWEI_PER_ETH; // at most 32 ETH at stake per validator
  const MAX_BALANCE_AT_STAKE = ACTIVE_VALIDATORS * MAX_EFFECTIVE_BALANCE;

  // Time parameters
  const SECONDS_PER_SLOT = 12;
  const SLOTS_PER_EPOCH = 32;
  const EPOCHS_PER_DAY = (24 * 60 * 60) / SLOTS_PER_EPOCH / SECONDS_PER_SLOT;
  const EPOCHS_PER_YEAR = 365.25 * EPOCHS_PER_DAY;

  // Base reward
  const BASE_REWARD_FACTOR = 64;
  const integerSqrt = (num: number) => Math.floor(Math.sqrt(num));
  const MAX_ISSUANCE_PER_EPOCH = Math.trunc(
    (BASE_REWARD_FACTOR * MAX_BALANCE_AT_STAKE) /
      integerSqrt(MAX_BALANCE_AT_STAKE),
  );
  const MAX_ISSUANCE_PER_DAY = MAX_ISSUANCE_PER_EPOCH * EPOCHS_PER_DAY;
  const MAX_ISSUANCE_PER_YEAR = MAX_ISSUANCE_PER_EPOCH * EPOCHS_PER_YEAR;

  const annualReward = MAX_ISSUANCE_PER_YEAR / GWEI_PER_ETH;
  const apr = MAX_ISSUANCE_PER_YEAR / GWEI_PER_ETH / totalEffectiveBalance;

  Log.debug(
    `ETH staked: ${totalEffectiveBalance}, active validator: ${ACTIVE_VALIDATORS}`,
  );
  Log.debug(
    `max issuance per epoch: ${MAX_ISSUANCE_PER_EPOCH / GWEI_PER_ETH} ETH`,
  );
  Log.debug(`max issuance per day: ${MAX_ISSUANCE_PER_DAY / GWEI_PER_ETH} ETH`);
  Log.debug(`max issuance per year: ${annualReward} ETH`);
  Log.debug(`APR: ${apr}`);

  return { annualReward, apr };
};

export const getValidatorRewards = () =>
  pipe(
    TE.Do,
    TE.apS(
      "lastEffectiveBalanceSum",
      BeaconBalances.getLastEffectiveBalanceSum(),
    ),
    TE.bindW("issuanceReward", ({ lastEffectiveBalanceSum }) =>
      pipe(getMaxIssuance(lastEffectiveBalanceSum), TE.right),
    ),
    TE.bindW("tipsReward", ({ lastEffectiveBalanceSum }) =>
      pipe(
        getTipsSinceGenesis(),
        T.map((tipsSinceGenesis) =>
          pipe(
            (tipsSinceGenesis / BeaconTime.getDaysSinceGenesis()) * 365.25,
            (tipsPerYear) =>
              gweiFromWei(tipsPerYear) *
              (gweiFromEth(32) / Number(lastEffectiveBalanceSum)),
            (tipsEarnedPerYear) => ({
              annualReward: tipsEarnedPerYear,
              apr: ethFromGwei(tipsEarnedPerYear) / 32,
            }),
          ),
        ),
        TE.fromTask,
      ),
    ),
    TE.map(({ issuanceReward, tipsReward }) => ({
      issuance: issuanceReward,
      tips: {
        annualReward: tipsReward.annualReward,
        apr: tipsReward.apr,
      },
      mev: {
        annualReward: 0.3 * 1e9,
        apr: 0.01,
      },
    })),
  );

export const validatorRewardsCacheKey = "validator-rewards";

export const updateValidatorRewards = () =>
  pipe(
    getValidatorRewards(),
    TE.chainTaskK((validatorRewards) =>
      KeyValueStore.storeValue(validatorRewardsCacheKey, validatorRewards),
    ),
    TE.chainFirstTaskK(() =>
      Db.sqlTNotify("cache-update", validatorRewardsCacheKey),
    ),
  );
