FROM node:12
WORKDIR /usr/src/app
EXPOSE 8080
COPY package*.json ./
RUN npm install
COPY main.js ./
CMD node main.js
