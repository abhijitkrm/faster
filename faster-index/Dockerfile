# Runtime image for a faster-index project: build with the project as the
# context's ./project directory, e.g.
#   docker build -t my-indexer --build-arg PROJECT=examples/erc20 .
FROM node:22-bookworm-slim AS build
WORKDIR /fi
COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY src ./src
RUN npx tsc -p . && npm prune --omit=dev

FROM node:22-bookworm-slim
ARG PROJECT=examples/erc20
RUN useradd --system --uid 10001 --create-home findex
COPY --from=build /fi/node_modules /fi/node_modules
COPY --from=build /fi/dist /fi/dist
COPY package.json /fi/
RUN ln -s /fi/dist/cli.js /usr/local/bin/faster-index && chmod +x /fi/dist/cli.js
COPY ${PROJECT} /app
RUN mkdir -p /app/node_modules && ln -s /fi /app/node_modules/faster-index && chown -R findex /app
USER findex
WORKDIR /app
ENV GRAPHQL_PORT=8080
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=5s --retries=3 CMD node -e "fetch('http://localhost:8080/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["faster-index", "start"]
