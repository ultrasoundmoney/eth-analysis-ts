import { differenceInDays } from "date-fns";
import { parseHTML } from "linkedom";
import fetch from "node-fetch";
import PQueue from "p-queue";
import ProgressBar from "progress";
import { sql } from "./db.js";
import * as Log from "./log.js";
import A from "fp-ts/lib/Array.js";
import { flow, pipe } from "fp-ts/lib/function.js";
import * as T from "fp-ts/lib/Task.js";
import * as O from "fp-ts/lib/Option.js";

export const fetchEtherscanName = async (
  address: string,
): Promise<string | undefined> => {
  const html = await fetch(`https://blockscan.com/address/${address}`).then(
    (res) => {
      if (res.status !== 200) {
        throw new Error(
          `bad request trying to fetch etherscan name, status: ${res.status}`,
        );
      }
      return res.text();
    },
  );

  const { document } = parseHTML(html);
  const etherscanPublicName = document.querySelector(".badge-secondary") as {
    innerText: string;
  } | null;

  return etherscanPublicName?.innerText;
};

const contractNameFetchQueue = new PQueue({
  concurrency: 1,
  interval: 1000,
  intervalCap: 1,
});

// type Contract = {
//   address: string;
//   name: string | null;
//   lastNameFetchAt: Date | null;
// };

export const getContractNameFetchedLongAgo = ({
  lastNameFetchAt,
}: {
  lastNameFetchAt: Date | null;
}): boolean =>
  lastNameFetchAt === null ||
  differenceInDays(new Date(), lastNameFetchAt) >= 3;

export const fetchMissingContractNames = async () => {
  const namelessContracts = await sql<
    { address: string; name: null; lastNameFetchAt: Date | null }[]
  >`
    SELECT address, last_name_fetch_at FROM contracts
    WHERE contracts.name IS NULL`;

  Log.info(`found ${namelessContracts.length} contracts without a name`);

  const contractsToFetch = namelessContracts.filter(
    getContractNameFetchedLongAgo,
  );

  Log.info(
    `there are ${contractsToFetch.length} contracts we haven't recently attempted to fetch a name for`,
  );

  const storeNameFound = (address: string, name: string) => sql`
    UPDATE contracts
    SET name = ${name},
        last_name_fetch_at = now()
    WHERE address = ${address}
  `;

  const storeNoNameFound = (address: string) => sql`
    UPDATE contracts
    SET last_name_fetch_at = now()
    WHERE address = ${address}
  `;

  const nameFetchIntervalId = setInterval(() => {
    Log.debug(
      `names left to fetch for ${contractNameFetchQueue.size} contract addresses`,
    );
  }, 8000);

  let bar: ProgressBar | undefined = undefined;
  if (process.env.SHOW_PROGRESS) {
    bar = new ProgressBar("[:bar] :rate/s :percent :etas", {
      total: contractsToFetch.length,
    });
  }

  await contractNameFetchQueue.addAll(
    contractsToFetch.map(
      ({ address }) =>
        () =>
          fetchEtherscanName(address)
            .then((name) =>
              name === undefined
                ? storeNoNameFound(address)
                : storeNameFound(address, name),
            )
            .then(() => bar?.tick()),
    ),
  );

  clearInterval(nameFetchIntervalId);
};

let onStartKnownAddresses: Set<string> | undefined = undefined;

const getKnownAddresses = (): T.Task<Set<string>> =>
  pipe(
    () => sql<{ address: string }[]>`SELECT address FROM contracts`,
    T.map((rows) =>
      pipe(
        rows,
        A.map(({ address }) => address),
        (knownAddresses) => new Set(knownAddresses),
      ),
    ),
    T.chainFirstIOK((knownAddresses) => () => {
      onStartKnownAddresses = knownAddresses;
    }),
  );

export const storeContracts = (addresses: string[]): T.Task<void> => {
  const writeAddressChunk = (chunk: { address: string }[]): T.Task<void> =>
    pipe(
      () => sql`
        INSERT INTO contracts
        ${sql(chunk, "address")}
        ON CONFLICT DO NOTHING
      `,
      T.map(() => undefined),
    );

  return pipe(
    onStartKnownAddresses,
    O.fromNullable,
    O.map(T.of),
    O.getOrElse(getKnownAddresses),
    T.map((knownAddresses) =>
      pipe(
        addresses,
        A.filter((address) => !knownAddresses.has(address)),
      ),
    ),
    T.chain(
      flow(
        A.map((address) => ({ address })),
        // We have more rows to insert than sql parameter substitution will allow. We insert in chunks.
        A.chunksOf(20000),
        T.traverseArray(writeAddressChunk),
        T.map(() => undefined),
      ),
    ),
  );
};
