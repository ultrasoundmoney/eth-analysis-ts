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

# Add AWS RDS certificate authority
ENV NODE_EXTRA_CA_CERTS=/app/global-bundle.pem
COPY global-bundle.pem .

COPY package.json .
COPY yarn.lock .
RUN ["yarn", "install", "--production"]

COPY --from=build /app/build/ build

CMD ["node", "build/serve_fee_users.js"]
