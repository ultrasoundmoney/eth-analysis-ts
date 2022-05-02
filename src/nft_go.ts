import { decodeWithError } from "./decoding.js";
import { A, D, E, O, pipe, TE } from "./fp.js";
import * as Fs from "./fs.js";
import * as Log from "./log.js";

// const nftGoApi = "https://api.nftgo.io/api/v1";
// const leaderboardUrl = formatUrl(nftGoApi, "/ranking/collections", {
//   offset: 0,
//   limit: 30,
//   by: "marketCap",
//   interval: "24h",
//   asc: -1,
//   rarity: -1,
//   fields: ["marketCap", "marketCapChange24h"].join(","),
// });

// collection page https://nftgo.io/collection/blitmap/overview
const Collection = D.struct({
  /** ETH | string */
  blockchain: D.string,
  contracts: D.array(D.string),
  link: D.string,
  logo: D.string,
  longDesc: D.string,
  marketCap: D.number,
  marketCapChange24h: D.nullable(D.number),
  medias: D.partial({
    twitter: D.nullable(D.string),
    discord: D.nullable(D.string),
    telegram: D.nullable(D.string),
    instagram: D.nullable(D.string),
    medium: D.nullable(D.string),
    youtube: D.nullable(D.string),
  }),
  name: D.string,
  slug: D.string,
});

export type Collection = D.TypeOf<typeof Collection>;

const LeaderboardResponse = D.struct({
  errorCode: D.literal(0),
  data: D.struct({
    total: D.number,
    list: D.array(Collection),
  }),
});

export type LeaderboardResponse = {
  errorCode: number;
  data: {
    total: number;
    list: Collection[];
  };
};

export const getRankedCollections = () =>
  pipe(
    // Blocked by CF
    // Fetch.fetchWithRetryJson(leaderboardUrl),
    Fs.readFileJson("./nftCollections.json"),
    TE.chainEitherKW(decodeWithError(LeaderboardResponse)),
    TE.chainEitherKW((res) =>
      pipe(
        res,
        (res) => res.data?.list,
        O.fromNullable,
        O.match(
          () => {
            Log.error("failed to fetch NftGo leaderboard", res);
            return E.left(
              new UnexpectedNftGoResponse("failed to fetch NftGo leaderboard"),
            );
          },
          (list) => E.right(list),
        ),
      ),
    ),
  );

// const getMarketCapUrl = () =>
//   formatUrl(nftGoApi, "/data/chart/marketcap", {
//     from: DateFns.getTime(new Date()),
//     to: DateFns.getTime(new Date()),
//     interval: "1h",
//   });

const MarketCapResponse = D.struct({
  errorCode: D.literal(0),
  data: D.struct({
    /** jsTimestamp */
    x: D.array(D.number),
    /** market cap in USD */
    y: D.array(D.number),
  }),
});

export type MarketCapResponse = D.TypeOf<typeof MarketCapResponse>;

export class UnexpectedNftGoResponse extends Error {}

export const getMarketCap = () =>
  pipe(
    // Blocked by CF
    // Fetch.fetchWithRetryJson(getMarketCapUrl()),
    Fs.readFileJson("./nftMarketCap.json"),
    TE.chainEitherKW(decodeWithError(MarketCapResponse)),
    TE.chainEitherKW((res) =>
      pipe(
        res.data?.y,
        O.fromNullable,
        O.chain(A.head),
        O.match(
          () => {
            Log.error("failed to fetch NftGo market cap", res);
            return E.left(
              new UnexpectedNftGoResponse("failed to fetch market cap"),
            );
          },
          (marketCap) => E.right(marketCap),
        ),
      ),
    ),
  );
