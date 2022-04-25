import PQueue from "p-queue";
import * as Duration from "../../duration.js";
import * as Fetch from "../../fetch.js";
import { B, O, pipe, T, TAlt, TE } from "../../fp.js";
import * as Log from "../../log.js";
import * as Queues from "../../queues.js";
import * as Twitter from "../../twitter.js";
import * as Contracts from "../contracts.js";
import { getShouldRetry } from "./attempts.js";

class NoKnownTwitterHandleError extends Error {}
class EmptyTwitterHandleError extends Error {}

const twitterProfileLastAttemptMap = new Map<string, Date>();

export const twitterProfileQueue = new PQueue({
  concurrency: 1,
  throwOnTimeout: true,
  timeout: Duration.millisFromSeconds(60),
});

export const addTwitterMetadata = (address: string) =>
  pipe(
    TE.Do,
    TE.apS(
      "twitterHandle",
      pipe(
        Contracts.getTwitterHandle(address),
        TE.fromTaskOption(() => new NoKnownTwitterHandleError()),
        TE.chainW((twitterHandle) =>
          twitterHandle.length === 0
            ? TE.left(new EmptyTwitterHandleError())
            : TE.right(twitterHandle),
        ),
      ),
    ),
    TE.bindW("profile", ({ twitterHandle }) =>
      pipe(
        Twitter.getProfileByHandle(twitterHandle),
        Queues.queueOnQueueWithTimeoutThrown(twitterProfileQueue),
      ),
    ),
    TE.bindW("imageUrl", ({ profile }) =>
      pipe(Twitter.getProfileImage(profile), (task) =>
        TE.fromTask<O.Option<string>, never>(task),
      ),
    ),
    (task) => task,
    TE.chainFirstIOK(() => () => {
      twitterProfileLastAttemptMap.set(address, new Date());
    }),
    TE.chainFirstIOK(({ profile, imageUrl }) => () => {
      Log.debug("updating twitter metadata", {
        description: profile.description,
        id: profile.id,
        imageUrl: imageUrl,
        name: profile.name,
      });
    }),
    TE.chainTaskK(({ profile, imageUrl }) =>
      TAlt.seqTPar(
        Contracts.setSimpleTextColumn(
          "twitter_image_url",
          address,
          O.toNullable(imageUrl),
        ),
        Contracts.setSimpleTextColumn("twitter_name", address, profile.name),
        Contracts.setSimpleTextColumn(
          "twitter_description",
          address,
          profile.description,
        ),
        Contracts.setSimpleTextColumn("twitter_id", address, profile.id),
      ),
    ),
    TE.chainTaskK(() => Contracts.updatePreferredMetadata(address)),
  );

export const addTwitterMetadataMaybe = (
  address: string,
  forceRefetch = false,
): T.Task<void> =>
  pipe(
    getShouldRetry(twitterProfileLastAttemptMap, address, forceRefetch),
    B.match(
      () => TE.of(undefined),
      () => addTwitterMetadata(address),
    ),
    TE.match(
      (e) => {
        if (
          e instanceof Twitter.InvalidHandleError ||
          e instanceof Twitter.ProfileNotFoundError ||
          (e instanceof Fetch.BadResponseError && e.status === 429)
        ) {
          Log.warn(e.message, e);
          return;
        }

        if (e instanceof NoKnownTwitterHandleError) {
          Log.debug(`no known twitter handle for contract ${address}`);
          return;
        }

        if (e instanceof Queues.TimeoutError) {
          Log.debug(
            `twitter metadata request timed out for contract ${address}`,
          );
          return;
        }

        Log.error(e.message, e);
      },
      () => undefined,
    ),
  );
