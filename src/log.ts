/* eslint-disable no-console */
import { pipe } from "fp-ts/lib/function.js";
import * as T from "fp-ts/lib/Task.js";
import kleur from "kleur";

const levelMap = {
  DEFAULT: 0,
  DEBUG: 100,
  INFO: 200,
  WARNING: 400,
  ERROR: 500,
  ALERT: 700,
} as const;

/**
 * Google Cloud Logging severity levels.
 * DEFAULT (0) The log entry has no assigned severity level.
 * DEBUG (100) Debug or trace information.
 * INFO (200) Routine information, such as ongoing status or performance.
 * NOTICE (300) Normal but significant events, such as start up, shut down, or a configuration change.
 * WARNING (400) Warning events might cause problems.
 * ERROR (500) Error events are likely to cause problems.
 * CRITICAL (600) Critical events cause more severe problems or outages.
 * ALERT (700) A person must take an action immediately.
 * EMERGENCY (800) One or more systems are unusable.
 */
export type Level = keyof typeof levelMap;

const prettySeverityMap: Record<Level, string> = {
  DEFAULT: "",
  WARNING: `${kleur.yellow("warn")}  - `,
  DEBUG: `${kleur.gray("debug")} - `,
  INFO: `${kleur.blue("info")}  - `,
  ERROR: `${kleur.red("error")} - `,
  ALERT: `${kleur.bgRed().white("alert")} - `,
};

const resolveAliasses = (level: string) =>
  level === "WARN" ? "WARNING" : level;

const logLevel = pipe(
  process.env["LOG_LEVEL"] as string | undefined,
  (logLevel) => logLevel ?? "WARNING",
  (level) => level.toUpperCase(),
  (level) => resolveAliasses(level) as Level,
);

const logFnMap: Record<Level, (...data: unknown[]) => void> = {
  DEFAULT: console.log,
  DEBUG: console.info,
  INFO: console.info,
  WARNING: console.warn,
  ERROR: console.error,
  ALERT: console.error,
};

const isPrettyLogEnabled =
  typeof process.env.PRETTY_LOG === "string" &&
  process.env.PRETTY_LOG !== "false";

export const log = (
  level = "DEFAULT" as Level,
  message: string,
  meta?: unknown,
): void => {
  if (levelMap[level] < levelMap[logLevel]) {
    return undefined;
  }

  const logFn = logFnMap[level];

  // Log to console during dev.
  if (process.env.ENV === "dev" || isPrettyLogEnabled) {
    const prettySeverity = prettySeverityMap[level];

    logFn(prettySeverity + message);

    if (meta !== undefined) {
      logFn(meta);
    }

    return undefined;
  }

  // Log json to stdout during non-dev.
  if (meta instanceof Error) {
    console.log(
      JSON.stringify({
        error_message: meta.message,
        error_name: meta.name,
        error_stack: meta.stack,
        level,
        message,
        // Log whatever extra properties meta still contains.
        meta: {
          ...meta,
          stack: undefined,
          message: undefined,
          name: undefined,
        },
        timestamp: new Date(),
      }),
    );
  } else {
    console.log(
      JSON.stringify({
        level,
        message,
        meta,
        timestamp: new Date(),
      }),
    );
  }
};

export const logIO =
  (level = "DEFAULT" as Level, message: string, meta?: unknown) =>
  () =>
    log(level, message, meta);

const makeLogWithLevel = (level: Level) => (message: string, meta?: unknown) =>
  log(level, message, meta);

const makeLogWithLevelIO =
  (level: Level) => (message: string, meta?: unknown) => () =>
    log(level, message, meta);

const makeLogWithLevelT = (level: Level) => (message: string, meta?: unknown) =>
  T.fromIO(logIO(level, message, meta));

export const debug = makeLogWithLevel("DEBUG");
export const info = makeLogWithLevel("INFO");
export const warn = makeLogWithLevel("WARNING");
export const error = makeLogWithLevel("ERROR");
export const alert = makeLogWithLevel("ALERT");

export const debugIO = makeLogWithLevelIO("DEBUG");
export const infoIO = makeLogWithLevelIO("INFO");
export const warnIO = makeLogWithLevelIO("WARNING");
export const errorIO = makeLogWithLevelIO("ERROR");
export const alertIO = makeLogWithLevelIO("ALERT");

export const debugT = makeLogWithLevelT("DEBUG");
export const infoT = makeLogWithLevelT("INFO");
export const warnT = makeLogWithLevelT("WARNING");
export const errorT = makeLogWithLevelT("ERROR");
export const alertT = makeLogWithLevelT("ALERT");
