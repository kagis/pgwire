FROM denoland/deno:alpine-1.34.0
COPY test_deno.js /tmp/test/
RUN set -x \
 && touch /tmp/mod.js \
 && touch /tmp/test/test.js \
 && deno cache /tmp/test/test_deno.js
