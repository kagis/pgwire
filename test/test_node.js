import { setup } from './test.js';
import * as pgwire from '../index.js';
import { deepStrictEqual as assertEquals } from 'assert';

const tests = [];

function test(name, fn) {
  tests.push({ name, fn });
}

setup({
  ...pgwire,
  test,
  assertEquals,
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
      console.log('%s - ok', name); // eslint-disable-line no-console
    } catch (err) {
      // console.log(err);
      console.log('%s - failed', name); // eslint-disable-line no-console
      throw err; // Error('test failed');
    } finally {
      clearTimeout(testTimeout);
    }
  }
}

runTests();
