import * as DateFns from "date-fns";
import * as DateFnsAlt from "./date_fns_alt.js";
import * as Duration from "./duration.js";
import * as Etherscan from "./etherscan.js";
import * as EthPriceEtherscan from "./eth_prices_etherscan.js";
import * as Log from "./log.js";
import { EthPrice } from "./etherscan.js";
import { pipe, T, TE } from "./fp.js";

let latestPrice: EthPrice | undefined = undefined;
let updateLatestPriceInterval: NodeJS.Timer | undefined = undefined;

export const getLatestPrice = (): T.Task<EthPrice> =>
  latestPrice === undefined
    ? pipe(
        setLatestPrice(),
        T.chainFirstIOK(() => () => {
          // On first request start updating periodically.
          if (updateLatestPriceInterval === undefined) {
            updateLatestPriceInterval = setInterval(
              () => setLatestPrice()(),
              Duration.milisFromSeconds(16),
            );
          }
        }),
      )
    : T.of(latestPrice);

const setLatestPrice = (): T.Task<EthPrice> =>
  pipe(
    Etherscan.getEthPrice(),
    TE.match(
      (error) => {
        Log.warn("failed to update eth price from etherscan", { error });

        if (latestPrice === undefined) {
          throw new Error(
            "failed to fetch etherscan eth price, can't initialize eth price",
          );
        }

        if (
          DateFns.differenceInSeconds(new Date(), latestPrice.timestamp) > 300
        ) {
          Log.error(
            "failed to update eth price from etherscan for more than five minutes! calculating with stale price.",
          );
        }

        return latestPrice;
      },
      (ethPrice) => {
        latestPrice = ethPrice;
        return ethPrice;
      },
    ),
  );

export const getNearestEtherscanPrice = async (
  maxDistanceInSeconds: number,
  blockMinedAt: Date,
): Promise<EthPrice | undefined> => {
  const latestPrice = await EthPriceEtherscan.getLatestPrice()();
  const distance = DateFnsAlt.secondsBetween(
    blockMinedAt,
    latestPrice.timestamp,
  );
  const isBlockYounger = distance < 0;
  const isWithinDistanceLimit = Math.abs(distance) <= maxDistanceInSeconds;

  if (isBlockYounger) {
    if (!isWithinDistanceLimit) {
      Log.error(
        `block is younger than latest price, diff: ${distance}s, exceeding limit`,
      );
      return undefined;
    }

    Log.debug(
      `block is younger than latest price, diff: ${distance}s, within limit`,
    );
    return latestPrice;
  } else {
    // Block is older than price.
    if (!isWithinDistanceLimit) {
      Log.warn(
        `block is older than latest price, diff: ${distance}s, exceeding limit`,
      );
      return undefined;
    }

    Log.debug(
      `block is older than latest price, diff: ${distance}s, within limit`,
    );
    return latestPrice;
  }
};
