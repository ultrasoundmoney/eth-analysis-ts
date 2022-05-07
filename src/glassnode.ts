import * as DateFns from "date-fns";
import QuickLRU from "quick-lru";
import * as UrlSub from "url-sub";
import * as Config from "./config.js";
import * as Fetch from "./fetch.js";
import { A, D, E, O, pipe, TE } from "./fp.js";
import { UnixTimestamp } from "./time.js";
import * as Duration from "./duration.js";
import { decodeWithError } from "./decoding.js";

const glassnodeApi = "https://api.glassnode.com";
const makeStakedDataUrl = () =>
  UrlSub.formatUrl(glassnodeApi, "/v1/metrics/eth2/staking_total_volume_sum", {
    a: "ETH",
    api_key: Config.getGlassnodeApiKey(),
    c: "NATIVE",
    f: "JSON",
    i: "24h",
    s: DateFns.getUnixTime(new Date("2020-11-03T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
  });

export const getStakedData = () =>
  Fetch.fetchWithRetryJson(makeStakedDataUrl());

const makeCurrentStakedUrl = () =>
  UrlSub.formatUrl(glassnodeApi, "/v1/metrics/eth2/staking_total_volume_sum", {
    a: "ETH",
    api_key: Config.getGlassnodeApiKey(),
    c: "NATIVE",
    f: "JSON",
    i: "1h",
    s: DateFns.getUnixTime(DateFns.subHours(new Date(), 2)),
    u: DateFns.getUnixTime(new Date()),
  });

type TotalValueStakedResponse = { t: UnixTimestamp; v: number }[];
const ethStakedCache = new QuickLRU<string, number>({
  maxSize: 1,
  maxAge: Duration.millisFromHours(4),
});
const ethStakedCacheKey = "eth-staked-cache-key";
export const getEthStaked = () =>
  pipe(
    ethStakedCache.get(ethStakedCacheKey),
    O.fromNullable,
    O.match(
      () =>
        pipe(
          Fetch.fetchWithRetryJson(makeCurrentStakedUrl()),
          TE.map((u) => u as TotalValueStakedResponse),
          TE.chainEitherK((res) =>
            pipe(
              res,
              A.last,
              O.map((row) => row.v),
              O.match(
                () =>
                  E.left(new Error("failed to get eth staked from Glassnode")),
                (ethStaked) => E.right(ethStaked),
              ),
            ),
          ),
          TE.chainFirstIOK((ethStaked) => () => {
            ethStakedCache.set(ethStakedCacheKey, ethStaked);
          }),
        ),
      (ethStaked) => TE.of(ethStaked),
    ),
  );

const makeCirculatingSupplyDataUrl = () =>
  UrlSub.formatUrl(glassnodeApi, "/v1/metrics/supply/current", {
    a: "ETH",
    api_key: Config.getGlassnodeApiKey(),
    c: "NATIVE",
    f: "JSON",
    i: "24h",
    s: DateFns.getUnixTime(new Date("2015-07-30T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
  });

const SupplyData = D.array(
  D.struct({
    t: D.number,
    v: D.number,
  }),
);

export type SupplyData = D.TypeOf<typeof SupplyData>;

export const getCirculatingSupplyData = () =>
  pipe(
    Fetch.fetchWithRetryJson(makeCirculatingSupplyDataUrl()),
    TE.chainEitherKW(decodeWithError(SupplyData)),
  );

const makeEthInSmartContractsDataUrl = () =>
  UrlSub.formatUrl(glassnodeApi, "/v1/metrics/distribution/supply_contracts", {
    a: "ETH",
    api_key: Config.getGlassnodeApiKey(),
    f: "JSON",
    i: "24h",
    s: DateFns.getUnixTime(new Date("2015-08-07T00:00:00Z")),
    u: DateFns.getUnixTime(new Date()),
  });

export const getLockedEthData = () =>
  Fetch.fetchWithRetryJson(makeEthInSmartContractsDataUrl());
