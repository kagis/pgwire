FROM node:11
RUN npm install --global eslint@6.8.0
CMD eslint .
