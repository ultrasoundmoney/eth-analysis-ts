import PQueue from "p-queue";
import * as Duration from "../../duration.js";
import * as Etherscan from "../../etherscan.js";
import { B, pipe, T, TAlt, TE, TO } from "../../fp.js";
import * as Log from "../../log.js";
import * as Queues from "../../queues.js";
import * as Contracts from "../contracts.js";
import { getShouldRetry } from "./attempts.js";
import * as CopyFromSimilar from "./copy_from_similar.js";

const etherscanNameTagLastAttemptMap = new Map<string, Date>();

export const etherscanNameTagQueue = new PQueue({
  carryoverConcurrencyCount: true,
  concurrency: 2,
  interval: Duration.millisFromSeconds(10),
  intervalCap: 3,
  throwOnTimeout: true,
  timeout: Duration.millisFromSeconds(60),
});

const addMetadata = (address: string) =>
  pipe(
    Etherscan.getNameTag(address),
    Queues.queueOnQueueWithTimeoutThrown(etherscanNameTagQueue),
    TE.chainFirstIOK(() => () => {
      etherscanNameTagLastAttemptMap.set(address, new Date());
    }),
    TE.chainTaskK((name) =>
      pipe(
        // The name is something like "Compound: cCOMP Token", we attempt to copy metadata from contracts starting with the same name before the colon i.e. /^compound.*/i.
        name.indexOf(":") === -1,
        B.match(
          () =>
            CopyFromSimilar.addMetadataFromSimilar(address, name.split(":")[0]),
          () => TO.of(undefined),
        ),
        // We expect category to be copied from similar metadata so only set is_bot.
        T.apSecond(
          TAlt.when(
            name.toLowerCase().startsWith("mev bot:"),
            Contracts.setIsBot(address, true),
          ),
        ),
        T.apSecond(
          Contracts.setSimpleTextColumn("etherscan_name_tag", address, name),
        ),
        T.chain(() => Contracts.updatePreferredMetadata(address)),
      ),
    ),
    TE.match(
      (e) => {
        if (e instanceof Etherscan.NoNameTagInHtmlError) {
          Log.warn("failed to read name tag from HTML", e);
          return;
        }

        if (e instanceof Queues.TimeoutError) {
          Log.debug(
            `Etherscan get name tag request timed out for contract ${address}`,
          );
          return;
        }

        Log.error("failed to get etherscan name tag", e);
      },
      (): void => undefined,
    ),
  );

export const checkForMetadata = (address: string, forceRefetch = false) =>
  pipe(
    getShouldRetry(etherscanNameTagLastAttemptMap, address, forceRefetch),
    (shouldRefetch) => TAlt.when(shouldRefetch, addMetadata(address)),
  );

// const etherscanMetaTitleLastAttemptMap: Record<string, Date | undefined> = {};

// export const etherscanMetaTitleQueue = new PQueue({
//   concurrency: 2,
//   throwOnTimeout: true,
//   timeout: Duration.millisFromSeconds(60),
// });

// const queueMetaTitleFetch = <E, A>(task: TE.TaskEither<E, A>) =>
//   pipe(
//     TE.tryCatch(
//       () => etherscanMetaTitleQueue.add(task),
//       () => new TimeoutError(),
//     ),
//     TE.chainW((e) => (E.isLeft(e) ? TE.left(e.left) : TE.right(e.right))),
//   );

// export const addEtherscanMetaTitle = async (
//   address: string,
//   forceRefetch = false,
// ): Promise<void> => {
//   const lastAttempted = etherscanMetaTitleLastAttemptMap[address];

//   if (
//     forceRefetch === false &&
//     lastAttempted !== undefined &&
//     DateFns.differenceInHours(new Date(), lastAttempted) < 12
//   ) {
//     return undefined;
//   }

//   const name = await queueMetaTitleFetch(Etherscan.getMetaTitle(address))();

//   if (E.isLeft(name)) {
//     if (name.left instanceof TimeoutError) {
//       return;
//     }

//     if (name.left instanceof Etherscan.NoMeaningfulTitleError) {
//       return;
//     }

//     Log.error("etherscan meta title fetch failed", name.left);
//     return;
//   }

//   Log.debug(`found etherscan meta title: ${name.right}, address: ${address}`);

//   etherscanMetaTitleLastAttemptMap[address] = new Date();

//   // The name is something like "Compound: cCOMP Token", we attempt to copy metadata from contracts starting with the same name before the colon i.e. /^compound.*/i.
//   if (name.right.indexOf(":") !== -1) {
//     const nameStartsWith = name.right.split(":")[0];
//     await addMetadataFromSimilar(address, nameStartsWith);
//   }

//   await Contracts.setSimpleTextColumn(
//     "etherscan_name_token",
//     address,
//     name.right,
//   )();
//   await Contracts.updatePreferredMetadata(address)();
// };
