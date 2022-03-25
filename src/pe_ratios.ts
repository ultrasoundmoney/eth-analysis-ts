import { formatUrl } from "url-sub";
import { sql, sqlT, sqlTNotify, sqlTVoid } from "./db.js";
import * as FetchAlt from "./fetch_alt.js";
import { A, E, flow, pipe, T, TE } from "./fp.js";
import * as Log from "./log.js";

export const peRatiosCacheKey = "pe-ratios-cache-key";

const symbols = [
  "AAPL",
  "AMZN",
  "DIS",
  "GOOGL",
  "INTC",
  "NFLX",
  "TSLA",
] as const;
type QuoteSymbol = typeof symbols[number];
type Quotes = Record<QuoteSymbol, number>;
type Quote = {
  trailingPE: number;
  symbol: string;
};
type QuoteApiResponse = {
  quoteResponse: {
    result: Quote[];
    error: null;
  };
};

const peRatioUrl = formatUrl("https://yfapi.net", "/v6/finance/quote", {
  region: "US",
  lang: "en",
  symbols: symbols.join(","),
});

const getPeRatios = () =>
  pipe(
    FetchAlt.fetchWithRetry(peRatioUrl, {
      headers: { "X-API-KEY": "YV30a3hdvZ6orr1vnm68O83gQBW2Si2l6wZLWYke" },
    }),
    TE.chainW((res) =>
      pipe(() => res.json() as Promise<QuoteApiResponse>, T.map(E.right)),
    ),
    TE.map(
      flow(
        (body) => body.quoteResponse.result,
        A.reduce(new Map<QuoteSymbol, number>(), (map, quote) => {
          return map.set(quote.symbol as QuoteSymbol, quote.trailingPE);
        }),
      ),
    ),
  );

export const updatePeRatios = () =>
  pipe(
    getPeRatios(),
    TE.map((map) => Object.fromEntries(map.entries())),
    TE.chainTaskK(
      (peRatios) => sqlTVoid`
        INSERT INTO key_value_store
          ${sql({
            key: peRatiosCacheKey,
            value: JSON.stringify(peRatios),
          })}
        ON CONFLICT (key) DO UPDATE SET
          value = excluded.value
        `,
    ),
    TE.chainTaskK(() => sqlTNotify("cache-update", peRatiosCacheKey)),
    TE.match(
      (e) => {
        Log.error("failed to update PE ratios", e);
      },
      () => undefined,
    ),
  )();

export const getPeRatiosCache = () =>
  pipe(
    sqlT<{ value: Quotes }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${peRatiosCacheKey}
    `,
    T.map((rows) => rows[0]?.value),
  );
