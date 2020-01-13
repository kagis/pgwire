FROM node:11-alpine
RUN npm install --global eslint@6.8.0
CMD eslint .
