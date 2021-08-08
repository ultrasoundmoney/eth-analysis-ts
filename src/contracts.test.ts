import { test } from "uvu";
import * as assert from "uvu/assert";
import { subHours } from "date-fns";
import * as Contracts from "./contracts.js";

test("filter contracts fetched more than three days ago", () => {
  const contracts = [
    { address: "0x0", lastNameFetchAt: subHours(new Date(), 71) },
    { address: "0x1", lastNameFetchAt: subHours(new Date(), 73) },
  ];

  const contractsToFetch = contracts.filter(
    Contracts.getContractNameFetchedLongAgo,
  );
  const addresses = contractsToFetch.map((c) => c.address);

  assert.equal(addresses, ["0x1"]);
});

test.run();
