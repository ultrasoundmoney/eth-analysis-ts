import * as Log from "./log.js";
import fetch from "node-fetch";
import urlcatModule from "urlcat";
import { E, pipe, T, TE } from "./fp.js";
import { getTwitterToken } from "./config.js";

// get "urlcat is not a function" otherwise.
const urlcat =
  urlcatModule || (urlcatModule as { default: typeof urlcatModule }).default;

type UserTwitterApiRaw = {
  profile_image_url: string;
};

export const toBiggerImage = (profileImageUrl: string) =>
  profileImageUrl.replace("normal", "reasonably_small");

const makeProfileByUsernameUrl = (handle: string) =>
  urlcat("https://api.twitter.com", "/2/users/by/username/:username", {
    username: handle,
    "user.fields": ["profile_image_url"].join(","),
  });

const apiErrorTypes = {
  notFound: "https://api.twitter.com/2/problems/resource-not-found",
} as const;

type ApiWrapper<A> = { data: A } | { errors: ApiError[] };

type ApiError = {
  title: string;
  type: string;
  detail: string;
  value?: string;
};

export type NotFoundError = {
  type: "NotFoundError";
  error: Error;
  handle: string;
};
type BadResponseError = { type: "BadResponseError"; error: Error };
type DecodeError = { type: "DecodeError"; error: Error };
type HttpError = { type: "HttpError"; error: Error };
type UnknownApiError = {
  type: "UnknownApiError";
  error: Error;
  title: string;
  apiErrorType: string;
};
type UnknownApiResponse = {
  type: "UnknownApiResponse";
  error: Error;
};
export type GetProfileError =
  | BadResponseError
  | DecodeError
  | HttpError
  | NotFoundError
  | UnknownApiError
  | UnknownApiResponse;

export const getProfileByHandle = (
  handle: string,
): TE.TaskEither<GetProfileError, UserTwitterApiRaw> =>
  pipe(
    TE.tryCatch(
      () =>
        fetch(makeProfileByUsernameUrl(handle), {
          headers: {
            Authorization: `Bearer ${getTwitterToken()}`,
          },
        }),
      (reason): HttpError => ({
        type: "HttpError",
        error: reason as Error,
      }),
    ),
    TE.chain(
      (res): TE.TaskEither<GetProfileError, ApiWrapper<UserTwitterApiRaw>> => {
        // A 200 response may still contain Api Errors
        if (res.status === 200) {
          return pipe(
            () => res.json() as Promise<ApiWrapper<UserTwitterApiRaw>>,
            T.map((a) => E.right(a)),
          );
        }

        // If a handle invalid chars twitter will return a 404 without a body.
        if (res.status === 404) {
          return TE.left({
            type: "NotFoundError",
            error: new Error(`404 - bad handle ${handle}`),
            handle,
          });
        }

        return TE.left({
          type: "BadResponseError",
          error: new Error(`bad response ${res.status} - ${res.statusText}`),
        });
      },
    ),
    TE.chain((body) => {
      if ("data" in body) {
        return TE.right(body.data);
      }

      if ("errors" in body) {
        // Twitter Api can return multiple errors but let us deal with one at a
        // time.
        const apiError = body.errors[0];
        if (apiError.type === apiErrorTypes.notFound) {
          return TE.left({
            type: "NotFoundError" as const,
            error: new Error(apiError.detail),
            handle,
          });
        }

        return TE.left({
          apiErrorType: apiError.type,
          error: new Error(apiError.detail),
          title: apiError.title,
          type: "UnknownApiError" as const,
        });
      }

      return TE.left({
        type: "UnknownApiResponse" as const,
        error: new Error("Unknown json body on 200 response"),
      });
    }),
  );

export const getImageUrl = (handle: string): Promise<string | undefined> => {
  return pipe(
    getProfileByHandle(handle),
    TE.map((user) => toBiggerImage(user.profile_image_url)),
    TE.match(
      (e) => {
        Log.error("failed to fetch image url", { ...e });
        return undefined;
      },
      (imageUrl) => imageUrl,
    ),
  )();
};
