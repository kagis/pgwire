import { inspect } from 'node:util';
import { test } from 'node:test';
import * as pgwire from '../index.js';
import { setup } from './test.js';

// TODO apply inspect options
globalThis.Deno = { inspect };

setup({ ...pgwire, test });
