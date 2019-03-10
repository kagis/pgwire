FROM node:11-alpine
RUN npm install --global eslint@5.15.1
CMD eslint .
