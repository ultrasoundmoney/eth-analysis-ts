import { pipe } from "fp-ts/lib/function.js";
import kleur from "kleur";

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
type Severity = keyof typeof severityMap;

const severityMap = {
  DEFAULT: 0,
  DEBUG: 100,
  INFO: 200,
  WARNING: 400,
  ERROR: 500,
} as const;

const prettySeverityMap: Record<Severity, string> = {
  DEFAULT: "",
  WARNING: `${kleur.yellow("warn")}  - `,
  DEBUG: `${kleur.gray("debug")} - `,
  INFO: `${kleur.blue("info")}  - `,
  ERROR: `${kleur.red("error")} - `,
};

const logLevel = pipe(
  process.env["LOG_LEVEL"] as Severity | undefined,
  (logLevel) => logLevel || "WARNING",
  (level) => level.toUpperCase() as Severity,
);

const logMap: Record<Severity, (message: string) => void> = {
  DEFAULT: console.log,
  DEBUG: console.info,
  INFO: console.info,
  WARNING: console.warn,
  ERROR: console.error,
};

export const log = (
  severity = "DEFAULT" as Severity,
  message: string,
  meta?: Record<string, unknown>,
) => {
  if (severityMap[severity] < severityMap[logLevel]) {
    return;
  }

  const logFn = logMap[severity];

  // Log to console during dev.
  if (process.env["ENV"] === "dev") {
    const prettySeverity = prettySeverityMap[severity];

    logFn(prettySeverity + message);

    if (meta !== undefined) {
      console.debug(meta);
    }

    return;
  }

  // Log json to stdout during non-dev.
  console.log(
    JSON.stringify({
      ...meta,
      severity,
      message,
    }),
  );
};

export const debug = (message: string, meta?: Record<string, unknown>) =>
  log("DEBUG", message, meta);

export const info = (message: string, meta?: Record<string, unknown>) =>
  log("INFO", message, meta);

export const warn = (message: string, meta?: Record<string, unknown>) =>
  log("WARNING", message, meta);

export const error = (message: string, meta?: Record<string, unknown>) =>
  log("ERROR", message, meta);

/**
 * reduce hash to six unique chars easier human reading.
 */
export const shortenHash = (hash: string): string => hash.slice(0, 8);
