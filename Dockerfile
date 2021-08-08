FROM node:16-alpine as build
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

# Add AWS RDS certificate authority
ENV NODE_EXTRA_CA_CERTS=/app/global-bundle.pem
COPY global-bundle.pem .

COPY package.json .
COPY yarn.lock .
RUN ["yarn", "install", "--production"]

COPY master_list.csv .
COPY --from=build /app/ .

CMD ["node", "src/serve_fees.js"]
