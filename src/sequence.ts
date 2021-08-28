import { sequenceS, sequenceT } from "fp-ts/lib/Apply.js";
import { ApplyPar, ApplySeq } from "fp-ts/lib/Task.js";

export const seqTPar = sequenceT(ApplyPar);
export const seqTSeq = sequenceT(ApplySeq);
export const seqSPar = sequenceS(ApplyPar);
export const seqSSeq = sequenceS(ApplySeq);
