import * as Contracts from "../contracts.js";
import * as Log from "../../log.js";
import { A, NEA, O, pipe, T, TO, TOAlt } from "../../fp.js";
import * as Db from "../../db.js";

type SimilarContract = {
  address: string;
  category: string | null;
  imageUrl: string | null;
  name: string | null;
  twitterHandle: string | null;
};

const getFirstKey = (
  similarContracts: SimilarContract[],
  key: keyof SimilarContract,
) =>
  pipe(
    similarContracts,
    A.map((contract) => contract[key]),
    A.map(O.fromNullable),
    A.compact,
    A.head,
  );

const storeFromSimilarContracts = (
  similarContracts: SimilarContract[],
  address: string,
  key: keyof SimilarContract,
  column: Contracts.SimpleTextColumn,
) =>
  pipe(
    getFirstKey(similarContracts, key),
    TO.fromOption,
    TO.chainTaskK((category) =>
      Contracts.setSimpleTextColumn(column, address, category),
    ),
  );

export const addMetadataFromSimilar = (
  address: string,
  nameStartsWith: string,
) =>
  pipe(
    T.fromIO(() => {
      Log.debug(
        `attempting to add similar metadata for ${nameStartsWith} - ${address}`,
      );
    }),
    // Sql helper does not like templates in templates.
    T.map(() => `${nameStartsWith}%`),
    T.chain(
      (nameStartsWithPlusWildcard) =>
        Db.sqlT<SimilarContract[]>`
          SELECT address, category, image_url, name, twitter_handle FROM contracts
          WHERE name ILIKE ${nameStartsWithPlusWildcard}
        `,
    ),
    T.map(NEA.fromArray),
    TO.chainFirstIOK((similarContracts) => () => {
      Log.debug(
        `found ${similarContracts.length} similar contracts, starting with ${nameStartsWith}`,
      );
    }),
    TO.chain((similarContracts) =>
      TOAlt.seqTPar(
        storeFromSimilarContracts(
          similarContracts,
          address,
          "category",
          "category",
        ),
        storeFromSimilarContracts(
          similarContracts,
          address,
          "imageUrl",
          "image_url",
        ),
        storeFromSimilarContracts(
          similarContracts,
          address,
          "twitterHandle",
          "twitter_handle",
        ),
      ),
    ),
    TOAlt.concatAllVoid,
  );
