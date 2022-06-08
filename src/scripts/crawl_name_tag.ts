import * as DateFns from "date-fns";
import { parseHTML } from "linkedom";
import * as Contracts from "../contracts/contracts.js";
import { addMetadataFromSimilar } from "../contracts/metadata/copy_from_similar.js";
import { getAddressesForMetadata } from "../contracts/metadata/metadata.js";
import { sql, sqlT, sqlTVoid } from "../db.js";
import * as Duration from "../duration.js";
import * as Fetch from "../fetch.js";
import { flow, O, pipe, Rec, T, TE, TEAlt, TO } from "../fp.js";
import * as GroupedAnalysis1 from "../grouped_analysis_1.js";
import * as Log from "../log.js";

let isUpdating = false;

if (typeof process.env.CF_COOKIE !== "string") {
  throw new Error("missing CF_COOKIE env var");
}

class NameNotFoundError extends Error {}

const fetchNameTag = (address: string) =>
  pipe(
    Fetch.fetchWithRetry(`https://blockscan.com/address/${address}`, {
      headers: {
        cookie: process.env.CF_COOKIE!,
        "user-agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.45 Safari/537.36",
      },
    }),
    TE.chainW((res) => TE.tryCatch(() => res.text(), TEAlt.decodeUnknownError)),
    TE.chainW((text) =>
      pipe(
        parseHTML(text),
        (html) => html.document,
        (document) =>
          document.querySelector(".badge-secondary") as {
            innerText: string;
          } | null,
        (etherscanPublicName) => etherscanPublicName?.innerText,
        O.fromNullable,
        TE.fromOption(() => new NameNotFoundError()),
      ),
    ),
  );

type NameTagAttempt = { lastAttempt: string; attempts: number };
type ContractNameTagAttemptMap = Record<string, NameTagAttempt>;

const lastFetchedKey = "name-tag-last-fetched";

const getContractNameTagAttemptMap = () =>
  pipe(
    sqlT<{ value: ContractNameTagAttemptMap }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${lastFetchedKey}
    `,
    T.map(
      flow(
        (rows) => rows[0]?.value,
        O.fromNullable,
        O.getOrElse(() => ({} as ContractNameTagAttemptMap)),
      ),
    ),
  );

const setContractNameTagAttemptMap = (
  contractNameTagAttemptMap: ContractNameTagAttemptMap,
) =>
  sqlTVoid`
    INSERT INTO key_value_store
      ${sql({
        key: lastFetchedKey,
        value: JSON.stringify(contractNameTagAttemptMap),
      })}
    ON CONFLICT (key) DO UPDATE SET
      value = excluded.value
  `;

const waitInMinutes = Duration.millisFromMinutes(8);

const getIsPastBackoff = (attempt: NameTagAttempt) => {
  // We use an exponential backoff here.
  const backoffPoint = DateFns.addMilliseconds(
    DateFns.parseISO(attempt.lastAttempt),
    waitInMinutes * 2 ** (attempt.attempts - 1),
  );
  return DateFns.isPast(backoffPoint);
};

const getNameTag = (address: string) =>
  pipe(
    sqlT<{ etherscanNameTag: string | null }[]>`
      SELECT etherscan_name_tag FROM contracts
      WHERE address = ${address}
    `,
    T.map((rows) => rows[0]?.etherscanNameTag),
    T.map(O.fromNullable),
    T.chain(
      O.match(
        () =>
          pipe(
            fetchNameTag(address),
            TE.match(
              (e) => {
                if (e instanceof NameNotFoundError) {
                  Log.debug(`name not found, skipping ${address}`);
                  return O.none;
                }

                if (e instanceof Fetch.BadResponseError) {
                  Log.error(e.message, e);
                  return O.none;
                }

                throw e;
              },
              (name) => O.some(name),
            ),
          ),
        (etherscanNameTag) => {
          Log.debug(`found existing name tag ${etherscanNameTag}, skipping`);
          return TO.none;
        },
      ),
    ),
  );

const updateNameTagForAddress = (
  address: string,
  contractNameTagAttemptMap: ContractNameTagAttemptMap,
) =>
  pipe(
    contractNameTagAttemptMap,
    Rec.lookup(address),
    O.match(
      () => getNameTag(address),
      (attempt) =>
        getIsPastBackoff(attempt)
          ? getNameTag(address)
          : pipe(
              Contracts.getName(address),
              TO.getOrElse(() => T.of(address)),
              T.chainFirstIOK((nameOrAddress) => () => {
                Log.debug(`${nameOrAddress} not yet past backoff, skipping`);
              }),
              T.map(() => O.none),
            ),
    ),
    T.chainFirstIOK(() => () => {
      contractNameTagAttemptMap[address] = {
        lastAttempt: DateFns.formatISO(new Date()),
        attempts: contractNameTagAttemptMap[address]?.attempts ?? 1,
      };
    }),
    TO.chainFirstTaskK((name) =>
      name.includes(":")
        ? addMetadataFromSimilar(address, name.split(":")[0])
        : T.of(undefined),
    ),
    TO.chainTaskK((name) =>
      pipe(
        // The name is something like "Compound: cCOMP Token", we attempt to copy metadata from contracts starting with the same name before the colon i.e. /^compound.*/i.
        Contracts.setSimpleTextColumn("etherscan_name_tag", address, name),
        T.chain(() => Contracts.updatePreferredMetadata(address)),
      ),
    ),
    T.chain(() => setContractNameTagAttemptMap(contractNameTagAttemptMap)),
  );

const updateLeaderboardMetadata = () =>
  pipe(
    T.Do,
    T.bind("flipIsUpdating", () =>
      T.fromIO(() => {
        isUpdating = true;
      }),
    ),
    T.apS("contractNameTagAttemptMap", getContractNameTagAttemptMap()),
    T.apS(
      "addresses",
      pipe(
        GroupedAnalysis1.getLatestAnalysis(),
        T.map(
          flow(
            (groupedAnalysis) =>
              getAddressesForMetadata(groupedAnalysis.leaderboards),
            (addresses) => Array.from(addresses.values()),
          ),
        ),
      ),
    ),
    T.chain(({ addresses, contractNameTagAttemptMap }) =>
      pipe(
        addresses,
        T.traverseSeqArray((address) =>
          updateNameTagForAddress(address, contractNameTagAttemptMap),
        ),
      ),
    ),
    T.chainIOK(() => () => {
      isUpdating = false;
    }),
  );

sql.listen("cache-update", async (payload) => {
  Log.debug(`DB notify cache-update, cache key: ${payload}`);

  if (payload === undefined) {
    Log.error("DB cache-update with no payload, skipping");
    return;
  }

  if (payload === GroupedAnalysis1.groupedAnalysis1CacheKey) {
    Log.debug("new grouped analysis complete");
    if (!isUpdating) {
      updateLeaderboardMetadata()();
    } else {
      Log.debug("already updating name tags, skipping");
    }
    return;
  }
});
