import * as Db from "./db.js";
import { flow, O, pipe, T, TE } from "./fp.js";
import * as NftGo from "./nft_go.js";

const collectionsKey = "nft-go-collections";
const marketCapKey = "nft-go-market-cap";

export const getRankedCollections = (): TE.TaskEither<
  Error,
  NftGo.Collection[]
> =>
  pipe(
    Db.sqlT<{ value: NftGo.Collection[] }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${collectionsKey}
    `,
    T.map(flow(O.fromNullableK((rows) => rows[0]?.value))),
    TE.fromTaskOption(
      () => new Error("failed to fall back to NftGo collections snapshot"),
    ),
  );

export const getMarketCap = () =>
  pipe(
    Db.sqlT<{ value: number }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${marketCapKey}
    `,
    T.map(flow(O.fromNullableK((rows) => rows[0]?.value))),
    TE.fromTaskOption(
      () => new Error("failed to fall back to NftGo market cap snapshot"),
    ),
  );

export const refreshRankedCollections = async () => {
  const rankedCollections = await NftGo.getRankedCollections();
  Db.sql`
    INSERT INTO key_value_store
      ${Db.values({
        key: collectionsKey,
        value: JSON.stringify(rankedCollections),
      })}
    ON CONFLICT (key) DO UPDATE SET
      value = excluded.value
  `;
};

export const refreshMarketCap = async () => {
  const marketCap = await NftGo.getMarketCap();
  Db.sql`
    INSERT INTO key_value_store
      ${Db.values({
        key: marketCapKey,
        value: JSON.stringify(marketCap),
      })}
    ON CONFLICT (key) DO UPDATE SET
      value = excluded.value
  `;
};
