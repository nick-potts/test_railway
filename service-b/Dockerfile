FROM node:20-alpine

RUN apk add --no-cache bind-tools

WORKDIR /app

COPY package.json server.js ./

EXPOSE 8080

CMD ["npm", "start"]
