import * as FetchAlt from "./fetch_alt.js";
import { A, E, O, pipe, TE } from "./fp.js";
import * as Log from "./log.js";

const leaderboardUrl =
  "https://api.nftgo.io/api/v1/ranking/collections?offset=0&limit=30&by=marketCap&interval=24h&asc=-1&rarity=-1&fields=marketCap,marketCapChange24h";

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

type LeaderboardResponse = {
  errorCode: number;
  data: {
    total: number;
    list: Collection[];
  };
};

export const getNftLeaderboard = () =>
  pipe(
    FetchAlt.fetchWithRetryJson<LeaderboardResponse>(leaderboardUrl, {
      headers: {
        "user-agent":
          "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.45 Mobile Safari/537.36",
      },
    }),
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

const marketCapUrl =
  "https://api.nftgo.io/api/v1/data/chart/marketcap?from=1647804665000&to=1647804665000&interval=1h";

type MarketCapResponse = {
  errorCode: 0;
  data: { y: number[] };
};

export class UnexpectedNftGoResponse extends Error {}

export const getMarketCap = () =>
  pipe(
    FetchAlt.fetchWithRetryJson<MarketCapResponse>(marketCapUrl, {
      headers: {
        "user-agent":
          "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.45 Mobile Safari/537.36",
      },
    }),
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
