import { setup } from './test.js';
import * as pgwire from '../index.js';
import { deepStrictEqual as assertEquals } from 'assert';
import net from 'net';
import { pipeline } from 'stream';

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

setup({
  ...pgwire,
  test,
  assertEquals,
  tcpproxy,
});

const filterRegexp = process.env.TEST_FILTER && new RegExp(process.env.TEST_FILTER);

async function runTests() {
  for (const { name, fn } of tests) {
    if (filterRegexp && !filterRegexp.test(name)) {
      continue;
    }
    let testTimeout;
    try {
      await Promise.race([fn(), new Promise((_, reject) => {
        testTimeout = setTimeout(reject, 60 * 1000, Error('test timeout'));
      })]);
      console.log('%s ... ok', name); // eslint-disable-line no-console
    } catch (err) {
      console.log('%s ... failed', name); // eslint-disable-line no-console
      throw err;
    } finally {
      clearTimeout(testTimeout);
    }
  }
}

async function tcpproxy({ listen, target, signal }) {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    signal.addEventListener('abort', _ => server.close());
    server.on('connection', inConn => {
      const outConn = net.connect({
        host: target.hostname,
        port: target.port,
      });
      pipeline(outConn, inConn, Boolean);
      pipeline(inConn, outConn, Boolean);
    });
    server.on('close', resolve);
    server.on('error', reject);
    server.listen({
      host: listen.hostname,
      port: listen.port,
      exclusive: true,
    });
  });
}

runTests();
