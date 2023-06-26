import * as Fetch from "../fetch.js";
import { GweiNumber } from "../eth_units.js";
import { pipe, TE } from "../fp.js";

type DateTimeString = string;

export type EthStaked = {
  timestamp: DateTimeString;
  sum: GweiNumber;
  slot: number;
};

export const getEthStaked = (): TE.TaskEither<
  Fetch.FetchError | Fetch.DecodeJsonError | Fetch.BadResponseError,
  EthStaked
> =>
  pipe(
    Fetch.fetchJson(
      "https://ultrasound.money/api/v2/fees/effective-balance-sum",
    ),
    TE.map((json) => json as EthStaked),
  );
