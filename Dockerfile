FROM node:16-alpine as build
WORKDIR /app

COPY package.json .
COPY yarn.lock .
RUN ["yarn", "install"]
COPY tsconfig.json .
COPY src/ src
RUN ["yarn", "build"]

FROM node:16-alpine as run
WORKDIR /app

COPY package.json .
COPY yarn.lock .
RUN ["yarn", "install", "--production"]

COPY --from=build /app/build/ build

CMD ["node", "build/serve_fee_users.js"]
