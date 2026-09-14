// Build the distributable sidecar: one bundled file, no install needed.
//
// The Spectrum SDK and its dependency tree are ~128 MB of node_modules; the
// bundle is ~8 MB and needs nothing beside it. Two shims make that work,
// both learned by breaking the bundle, not by reading docs:
//
// 1. The createRequire banner. Bundled CJS deps call `require()` on node
//    builtins (opentelemetry wants async_hooks); esbuild's ESM output
//    replaces that with a shim that throws "Dynamic require is not
//    supported". A real `require` in scope makes esbuild use it.
//
// 2. The peer stubs. @photon-ai/advanced-imessage pre-flights its optional
//    gRPC peers with `import.meta.resolve("nice-grpc")` — a *filesystem*
//    check, run against the bundle's own location, before the dynamic import
//    that esbuild has already inlined. The stub packages exist so that
//    resolve() succeeds; nothing is ever loaded from them.
//
// Verified against the live service: the bundled sidecar constructs Spectrum
// with real credentials, answers 401 to an unauthenticated /inbound, and
// holds the stream open with one.
import { build } from "esbuild";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const dist = join(here, "dist");

await build({
  entryPoints: [join(here, "index.mjs")],
  bundle: true,
  platform: "node",
  format: "esm",
  banner: {
    js: "import{createRequire as __cr}from'node:module';const require=__cr(import.meta.url);",
  },
  outfile: join(dist, "index.mjs"),
});

for (const name of ["nice-grpc", "nice-grpc-common", "@grpc/grpc-js"]) {
  const dir = join(dist, "node_modules", ...name.split("/"));
  mkdirSync(dir, { recursive: true });
  writeFileSync(
    join(dir, "package.json"),
    JSON.stringify({ name, version: "0.0.0", main: "index.js" }) + "\n",
  );
  writeFileSync(join(dir, "index.js"), "module.exports = {};\n");
}

console.log("photon-sidecar: bundled to dist/index.mjs");
