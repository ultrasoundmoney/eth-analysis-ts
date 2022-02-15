import urlSub from "url-sub";
import { sql, sqlT, sqlTNotify, sqlTVoid } from "./db.js";
import * as FetchAlt from "./fetch_alt.js";
import { A, E, flow, pipe, T, TE } from "./fp.js";
import * as Log from "./log.js";

export const peRatiosCacheKey = "pe-ratios-cache-key";

type QuoteSymbol = "AAPL" | "GOOGL" | "NFLX" | "TSLA";
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

const getPeRatios = () =>
  pipe(
    FetchAlt.fetchWithRetry(
      urlSub.formatUrl("https://yfapi.net", "/v6/finance/quote", {
        region: "US",
        lang: "en",
        symbols: "AAPL,GOOGL,NFLX,TSLA",
      }),
      { headers: { "X-API-KEY": "muzty6czcs6YcpxoSb27K5RmzBdXgIO8a33mBs3T" } },
    ),
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
