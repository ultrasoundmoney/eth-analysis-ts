export type BaseError = {
  message: string;
  name: string;
  stack?: string;
};

/**
 * We'd like type checking to be aware which errors are returned, returning Error anywhere breaks tracking of any subclasses so we don't use it. Instead, we wrap errors in our own, tagged, construct.
 */
export type TaggedError = {
  type: "TaggedError";
} & BaseError;

export type TaggedUnknownError = {
  name: "TaggedUnknownError";
  type: "TaggedUnknownError";
} & BaseError;

export const fromError = (e: Error): TaggedError => ({
  message: e.message,
  name: e.name,
  stack: e.stack,
  type: "TaggedError",
});

// Technically whatever we call can throw anything. We really don't know if we're catching an error, or something else, for the something else case we use this error.
export const fromUnknown = (u: unknown): TaggedUnknownError => ({
  message: String(u),
  name: "TaggedUnknownError",
  type: "TaggedUnknownError",
});
