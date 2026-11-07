FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci; else npm install; fi
COPY index.html vite.config.js tsconfig.json postcss.config.js tailwind.config.js ./
COPY public ./public
COPY src ./src
RUN npx tsc --noEmit -p . && npm run build

FROM nginx:1.27-alpine
ENV FASTER_UPSTREAM=faster:3000 FASTER_API_URL=
COPY deploy/default.conf.template /etc/nginx/templates/default.conf.template
COPY deploy/40-faster-config.sh /docker-entrypoint.d/40-faster-config.sh
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --retries=3 CMD wget -qO- http://127.0.0.1:8080/ >/dev/null || exit 1
