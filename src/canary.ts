import _ from "lodash";
import fetch from "node-fetch";
import * as Duration from "./duration.js";
import * as Log from "./log.js";

const opsGenieApiKey = "5ee39424-ba9e-4206-93f8-50a240ab84b2";

type CanaryType = "block" | "leaderboard";
const cage: Record<CanaryType, NodeJS.Timeout | undefined> = {
  block: undefined,
  leaderboard: undefined,
};

const durationMilis = Duration.millisFromMinutes(6);

const fireAlarm = _.debounce(
  async () => {
    Log.alert(`canary dead, no block for ${durationMilis / 1000}s`);

    const res = await fetch("https://api.opsgenie.com/v2/alerts", {
      method: "POST",
      headers: {
        Authorization: `GenieKey ${opsGenieApiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        message: `no block for more than ${durationMilis / 1000 / 60} minutes!`,
      }),
    });

    if (!res.ok) {
      const body = (await res.json()) as {
        message: string;
        took: number;
        requestId: string;
      };
      throw new Error(`OpsGenie alert request failed! ${body.message}`);
    }
  },
  Duration.millisFromMinutes(4),
  { leading: true },
);

export const releaseCanary = (): void => {
  cage.block = setTimeout(fireAlarm, durationMilis);
};

export const resetCanary = () => {
  Log.debug("resetting block canary");

  const timeout = cage.block;
  if (timeout) {
    timeout.refresh();
  }
};
