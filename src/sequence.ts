import { sequenceT } from "fp-ts/lib/Apply.js";
import { ApplyPar, ApplySeq } from "fp-ts/lib/Task.js";

export const seqTPar = sequenceT(ApplyPar);
export const seqTSeq = sequenceT(ApplySeq);
export const seqSPar = sequenceT(ApplyPar);
export const seqSSeq = sequenceT(ApplySeq);
