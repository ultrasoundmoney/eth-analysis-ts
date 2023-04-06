import { DecodeError, decodeWithError } from "./decoding.js";
import { D, T, TE, pipe } from "./fp.js";
import * as Fs from "fs/promises";

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
  link: D.nullable(D.string),
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

export const getRankedCollections: TE.TaskEither<DecodeError, Collection[]> =
  pipe(
    // File does not exist outside dev.
    () => Fs.readFile("./nftCollections.json", "utf8"),
    T.map(JSON.parse),
    T.map(decodeWithError(LeaderboardResponse)),
    TE.map((decoded) => decoded.data.list),
  );

const MarketCapResponse = D.struct({
  errorCode: D.literal(0),
  data: D.struct({
    marketCap: D.struct({
      meta: D.struct({
        /** market cap in USD */
        value: D.number,
      }),
    }),
  }),
});

export type MarketCapResponse = D.TypeOf<typeof MarketCapResponse>;

export class UnexpectedNftGoResponse extends Error {}

export const getMarketCap: TE.TaskEither<DecodeError, number> = pipe(
  // File does not exist outside dev.
  () => Fs.readFile("./nftMarketCap.json", "utf8"),
  T.map(JSON.parse),
  T.map(decodeWithError(MarketCapResponse)),
  TE.map((decoded) => decoded.data.marketCap.meta.value),
);
