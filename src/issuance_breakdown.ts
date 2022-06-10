import * as Casing from "./casing.js";
import { pipe, TO } from "./fp.js";
import * as KeyValueStore from "./key_value_store.js";

export const issuanceBreakdownCacheKey = "issuance-breakdown";

export const getIssuanceBreakdown = () =>
  pipe(
    KeyValueStore.getValue<Record<string, string>>(issuanceBreakdownCacheKey),
    TO.map(Casing.camelCaseKeys),
  );
