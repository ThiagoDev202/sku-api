FROM node:24-bookworm-slim AS development

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund

COPY . .

RUN npm run build

EXPOSE 4000

CMD ["npm", "run", "start:api"]

FROM development AS build

RUN npm run build

FROM node:24-bookworm-slim AS production

WORKDIR /app
ENV NODE_ENV=production

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

COPY --from=build /app/dist ./dist

EXPOSE 4000

CMD ["npm", "run", "start:api"]
