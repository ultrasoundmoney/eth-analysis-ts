import { A, O, pipe, T } from "./fp.js";
import { readFile } from "fs/promises";
import neatCsv from "neat-csv";
import { HistoricPrice } from "./coingecko.js";

type GeminiRow = {
  "Unix Timestamp": string;
  Date: string;
  Close: string;
};

let geminiPrices: HistoricPrice[] | undefined = undefined;

const getGeminiPrices = (): T.Task<HistoricPrice[]> =>
  pipe(
    () => readFile("./gemini_ETHUSD_2021_1min.csv", "utf8"),
    T.chain((str) => () => neatCsv<GeminiRow>(str, { skipLines: 1 })),
    T.map(
      A.map((row) => [Number(row["Unix Timestamp"]), Number(row["Close"])]),
    ),
  );

export const getGeminiPricesWithCache = (): T.Task<HistoricPrice[]> =>
  pipe(
    geminiPrices,
    O.fromNullable,
    O.match(
      () =>
        pipe(
          getGeminiPrices(),
          T.chainFirstIOK((prices) => () => {
            geminiPrices = prices;
          }),
        ),
      (prices) => T.of(prices),
    ),
  );
