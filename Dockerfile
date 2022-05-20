FROM node:18-alpine as build
WORKDIR /app

RUN apk add g++ make py3-pip && rm -rf /var/cache/apk/*
COPY package.json .
COPY yarn.lock .
RUN ["yarn", "install"]
COPY tsconfig.json .
COPY tsconfig.prod.json .
COPY src/ src
COPY migrations/ migrations
RUN ["yarn", "build:prod"]

FROM node:18-alpine as run
WORKDIR /app

RUN apk add g++ make py3-pip && rm -rf /var/cache/apk/*
COPY package.json .
COPY yarn.lock .
RUN ["yarn", "install", "--production"]

COPY --from=build /app/ .

CMD ["node", "src/serve/serve.js"]
