import { setup } from './test.js';
import * as pgwire from '../mod.js';
import { assertEquals } from 'https://deno.land/std@0.120.0/testing/asserts.ts';

setup({
  ...pgwire,
  test: Deno.test.bind(Deno),
  assertEquals,
});
