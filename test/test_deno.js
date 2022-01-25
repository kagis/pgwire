import { setup } from './test.js';
import * as pgwire from '../mod.js';
import { assertEquals, assertObjectMatch } from 'https://deno.land/std@0.122.0/testing/asserts.ts';

setup({
  ...pgwire,
  test: Deno.test.bind(Deno),
  assertEquals,
  assertObjectMatch,
});
