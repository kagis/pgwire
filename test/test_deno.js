import { setup } from './test.js';
import * as pgwire from '../mod.js';

const test = Deno.test.bind(Deno);

setup({ ...pgwire, test });
