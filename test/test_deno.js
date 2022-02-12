import { setup } from './test.js';
import * as pgwire from '../mod.js';
import { assertEquals, assertObjectMatch } from 'https://deno.land/std@0.125.0/testing/asserts.ts';
import { copy } from 'https://deno.land/std@0.125.0/streams/conversion.ts';

setup({
  ...pgwire,
  test: Deno.test.bind(Deno),
  assertEquals,
  assertObjectMatch,
  tcpproxy,
});

async function tcpproxy({ listen, target, signal }) {
  if (signal.aborted) return;
  const listener = Deno.listen(listen);
  signal.addEventListener('abort', _ => listener.close());
  const pending = [];
  for await (const inConn of listener) {
    const outConn = await Deno.connect(target);
    pending.push(
      Promise.all([
        copy(inConn, outConn).then(_ => outConn.closeWrite()),
        copy(outConn, inConn).then(_ => inConn.closeWrite()),
      ]).then(_ => {
        inConn.close();
        outConn.close();
      })
    );
  }
  await Promise.all(pending);
}
