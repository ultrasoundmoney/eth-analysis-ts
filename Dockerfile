FROM node:17-alpine as build
WORKDIR /app

COPY package.json .
COPY yarn.lock .
RUN ["yarn", "install"]
COPY tsconfig.json .
COPY tsconfig.prod.json .
COPY src/ src
RUN ["yarn", "build:prod"]

FROM node:16-alpine as run
WORKDIR /app

COPY package.json .
COPY yarn.lock .
RUN ["yarn", "install", "--production"]

COPY --from=build /app/ .

CMD ["node", "src/serve/serve_fees.js"]
