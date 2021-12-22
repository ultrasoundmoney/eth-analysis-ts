import * as DateFns from "date-fns";
import fetch, { RequestInfo } from "node-fetch";
import urlcatM from "urlcat";
import * as Config from "./config.js";
import { E, pipe, T, TE } from "./fp.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

const stakedUrl = urlcat(
  "https://api.glassnode.com/v1/metrics/eth2/staking_total_volume_sum",
  {
    a: "ETH",
    s: DateFns.getUnixTime(new Date("2020-11-03T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
    i: "24h",
    f: "JSON",
    c: "NATIVE",
  },
);

export class BadResponseError extends Error {}

const fetchData = (url: RequestInfo): TE.TaskEither<Error, unknown> =>
  pipe(
    () =>
      fetch(url, {
        headers: { "X-Api-Key": Config.getGlassnodeApiKey() },
      }),
    T.chain((res) => {
      if (res.status !== 200) {
        return TE.left(
          new BadResponseError(
            `glassnode api fetch, bad response: ${res.status}, url: ${url}`,
          ),
        );
      }

      return pipe(() => res.json(), T.map(E.right));
    }),
  );

export const getStakedData = () => fetchData(stakedUrl);

const circulatingSupplyUrl = urlcat(
  "https://api.glassnode.com/v1/metrics/supply/current",
  {
    a: "ETH",
    s: DateFns.getUnixTime(new Date("2015-07-30T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
    i: "24h",
    f: "JSON",
    c: "NATIVE",
  },
);

export const getCirculatingSupplyData = () => fetchData(circulatingSupplyUrl);

const ethInSmartContractsPercentUrl = urlcat(
  "https://api.glassnode.com/v1/metrics/distribution/supply_contracts",
  {
    a: "ETH",
    s: DateFns.getUnixTime(new Date("2015-08-07T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
    i: "24h",
    f: "JSON",
  },
);

export const getLockedEthData = () => fetchData(ethInSmartContractsPercentUrl);
