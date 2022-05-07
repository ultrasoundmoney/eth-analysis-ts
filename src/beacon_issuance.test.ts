import test from "ava";
import * as BeaconTime from "./beacon_time.js";

test("converts slot to timestamp", (t) => {
  t.deepEqual(
    BeaconTime.getTimestampFromSlot(0),
    new Date("2020-12-01T12:00:23Z"),
  );
  t.deepEqual(
    BeaconTime.getTimestampFromSlot(1),
    new Date("2020-12-01T12:00:35Z"),
  );
});

test("converts slot to UTC day of month", (t) => {
  t.is(BeaconTime.getDayOfMonthFromSlot(0), 1);

  // 2020-12-01T23:59:59.000Z
  t.is(BeaconTime.getDayOfMonthFromSlot(3598), 1);
  // 2020-12-02T00:00:11.000Z
  t.is(BeaconTime.getDayOfMonthFromSlot(3599), 2);
});
