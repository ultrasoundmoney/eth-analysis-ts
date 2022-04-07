import * as DateFns from "date-fns";
import { formatUrl } from "url-sub";
import * as FetchAlt from "./fetch_alt.js";
import { A, E, O, pipe, TE } from "./fp.js";
import * as Log from "./log.js";

const nftGoApi = "https://api.nftgo.io/api/v1";
const leaderboardUrl = formatUrl(
  nftGoApi,
  "/ranking/collections?offset=0&limit=30&by=marketCap&interval=24h&asc=-1&rarity=-1&fields=marketCap,marketCapChange24h",
  {},
);

// collection page https://nftgo.io/collection/blitmap/overview
export type Collection = {
  blockchain: "ETH" | string;
  contracts: string[];
  link: string;
  logo: string;
  longDesc: string;
  marketCap: number;
  marketCapChange24h: number;
  medias: {
    twitter?: string;
    discord?: string;
    telegram?: string | null;
    instagram?: string | null;
    medium?: string | null;
    youtube?: string | null;
  };
  name: string;
  slug: string;
};

export type LeaderboardResponse = {
  errorCode: number;
  data: {
    total: number;
    list: Collection[];
  };
};

export const getRankedCollections = () =>
  pipe(
    FetchAlt.fetchWithRetryJson(leaderboardUrl, {
      headers: {
        "User-Agent": "HTTPie/3.0.2",
      },
    }),
    TE.map((u) => u as LeaderboardResponse),
    TE.chainEitherK((res) =>
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

const getMarketCapUrl = () =>
  formatUrl(nftGoApi, "/data/chart/marketcap", {
    from: DateFns.getTime(new Date()),
    to: DateFns.getTime(new Date()),
    interval: "1h",
  });

export type MarketCapResponse = {
  errorCode: 0;
  data: { y: number[] };
};

export class UnexpectedNftGoResponse extends Error {}

export const getMarketCap = () =>
  pipe(
    FetchAlt.fetchWithRetryJson(getMarketCapUrl(), {
      headers: {
        "User-Agent": "HTTPie/3.0.2",
      },
    }),
    TE.map((u) => u as MarketCapResponse),
    TE.chainEitherK((res) =>
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
