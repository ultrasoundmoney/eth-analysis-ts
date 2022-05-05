import { setInterval } from "timers/promises";
import * as Duration from "./duration.js";
import { pipe, TE, TEAlt } from "./fp.js";
import * as Log from "./log.js";
import * as TotalValueSecured from "./total-value-secured/total_value_secured.js";

const everyMinuteIterator = setInterval(
  Duration.millisFromMinutes(1),
  new Date(),
);

const everyMinuteDo = async () => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of everyMinuteIterator) {
    await pipe(
      TotalValueSecured.updateTotalValueSecured(),
      TE.match(
        (e) => {
          Log.error("falied to update total value secured", e);
        },
        (): void => {
          Log.info("successfully updated total value secured");
        },
      ),
    )();
  }
};

export const init = () =>
  pipe(
    TotalValueSecured.updateTotalValueSecured(),
    TE.chainFirstIOK(() => () => {
      everyMinuteDo();
    }),
    TEAlt.getOrThrow,
  );

await init()();
