import * as Retry from "retry-ts";
import * as UrlSub from "url-sub";
import { decodeEmptyString } from "./decoding.js";
import * as Fetch from "./fetch.js";
import { A, D, NEA, O, pipe, TE } from "./fp.js";

const coinGeckoApiUrl = "https://api.coingecko.com/api/v3";

const retryPolicy = Retry.Monoid.concat(
  Retry.exponentialBackoff(2000),
  Retry.limitRetries(7),
);

export type PriceResponse = {
  ethereum: {
    usd: number;
    usd_24h_change: number;
    usd_market_cap: number;
    btc: number;
    btc_24h_change: number;
  };
  bitcoin: {
    usd: number;
    usd_24h_change: number;
    usd_market_cap: number;
  };
  "tether-gold": {
    usd: number;
    usd_24h_change: number;
    usd_market_cap: number;
  };
};

const simplePriceUrl = UrlSub.formatUrl(coinGeckoApiUrl, "/simple/price", {
  ids: ["ethereum", "bitcoin", "tether-gold"].join(","),
  vs_currencies: ["usd", "btc"].join(","),
  include_market_cap: "true",
  include_24hr_change: "true",
});

export const getSimpleCoins = () =>
  pipe(
    Fetch.fetchWithRetryJson(simplePriceUrl, undefined, {
      retryPolicy,
    }),
    TE.map((u) => u as PriceResponse),
  );

export type IndexCoin = {
  id: string;
  symbol: string;
  name: string;
  platforms: Partial<Record<string, string>>;
};

const coinListUrl = UrlSub.formatUrl(coinGeckoApiUrl, "/coins/list", {
  include_platform: "true",
});

export const getCoinList = () =>
  pipe(
    Fetch.fetchWithRetryJson(coinListUrl, undefined, {
      retryPolicy,
    }),
    TE.map((u) => u as IndexCoin[]),
  );

type ISO8601String = string;

export type CoinMarket = {
  circulating_supply: number;
  current_price: number;
  id: string;
  image: string;
  last_updated: ISO8601String;
  name: string;
  symbol: string;
  total_supply: number;
  twitter_handle: string | null;
};

const coinMarketUrl = UrlSub.formatUrl(
  coinGeckoApiUrl,
  "/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1&sparkline=false",
  {
    vs_currency: "usd",
    order: "market_cap_desc",
    per_page: 250,
    page: 1,
    sparkline: false,
  },
);

export const getTopCoinMarkets = () =>
  pipe(
    Fetch.fetchWithRetryJson(coinMarketUrl, undefined, {
      retryPolicy,
    }),
    TE.map((u) => u as CoinMarket[]),
  );

const CoinResponse = D.struct({
  name: D.string,
  categories: pipe(decodeEmptyString, D.nullable, D.array),
  image: D.struct({
    large: D.string,
  }),
  links: D.struct({
    twitter_screen_name: decodeEmptyString,
  }),
});

const makeCoinUrl = (id: string) =>
  UrlSub.formatUrl(coinGeckoApiUrl, "/coins/:id", {
    id,
    localization: false,
    tickers: false,
    market_data: false,
  });

export const getCoin = (id: string) =>
  pipe(
    Fetch.fetchWithRetryJson(makeCoinUrl(id), undefined, {
      retryPolicy,
    }),
    TE.chainEitherKW(CoinResponse.decode),
    TE.map((res) => ({
      name: res.name,
      categories: pipe(
        res.categories,
        A.map(O.fromNullable),
        A.compact,
        NEA.fromArray,
        O.toNullable,
      ),
      image_url:
        res.image.large === "missing_large.png" ? null : res.image.large,
      twitter_handle: res.links.twitter_screen_name,
    })),
  );
