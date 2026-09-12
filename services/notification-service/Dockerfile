FROM node:22-alpine

WORKDIR /app

RUN apk add --no-cache curl

COPY package*.json ./
RUN npm install --omit=dev

COPY src/ ./src/

EXPOSE 8002

CMD ["npm", "start"]
