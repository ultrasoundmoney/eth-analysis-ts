import { differenceInDays } from "date-fns";
import { parseHTML } from "linkedom";
import fetch from "node-fetch";
import PQueue from "p-queue";
import ProgressBar from "progress";
import { sql } from "./db.js";
import * as Log from "./log.js";
import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import * as T from "fp-ts/lib/Task.js";

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

export const storeContracts = (addresses: string[]): T.Task<void> => {
  if (addresses.length === 0) {
    return T.of(undefined);
  }

  return pipe(
    addresses,
    A.map((address) => ({ address })),
    (addresses) => () =>
      sql`
        INSERT INTO contracts
        ${sql(addresses, "address")}
        ON CONFLICT DO NOTHING
      `,
    T.map(() => undefined),
  );
};
