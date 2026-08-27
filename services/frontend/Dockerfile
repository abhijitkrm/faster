FROM node:18-alpine AS build

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY src ./src
COPY index.html vite.config.js tsconfig.json ./

ARG VITE_API_URL=http://localhost:3000
ENV VITE_API_URL=$VITE_API_URL

RUN npm run build

FROM node:18-alpine

WORKDIR /app

RUN npm install -g serve

COPY --from=build /app/dist ./dist

EXPOSE 5173

CMD ["serve", "-s", "dist", "-l", "5173"]
