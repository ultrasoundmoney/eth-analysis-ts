import fs from "fs";
import { pipe, TE } from "./fp.js";

export const readFile = TE.tryCatchK(
  (path: fs.PathOrFileDescriptor) =>
    new Promise<string>((resolve, reject) => {
      fs.readFile(path, "utf8", (err, data) => {
        if (err !== null) {
          reject(err);
        }

        resolve(data);
      });
    }),
  (e) => e as NodeJS.ErrnoException,
);

export const readFileJson = (path: fs.PathOrFileDescriptor) =>
  pipe(
    readFile(path),
    TE.map((text) => JSON.parse(text) as unknown),
  );
