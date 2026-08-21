# Acknowledgements

This project is a port of **[apify/crawlee](https://github.com/apify/crawlee)**.

## Licence and copyright

- apify/crawlee is licensed under the **Apache License 2.0**. Copyright 2018 Apify Technologies
  s.r.o. (`crawlee-src/LICENSE.md:189`).
- **Nothing was copied verbatim.** Every Java file under `crawlee-akka/src` was written fresh
  against the behaviour read out of the TypeScript source; no source text, comments, or test
  fixtures were transcribed. Method and class names in this port's Javadoc cite the source
  file and line range they were read from (e.g. `session.ts:253-262`), which is citation, not
  copying.
- **Behaviour is derived throughout**, plainly: the retry decision, session scoring rules, and
  backoff/crawl-delay arithmetic in `crawlee-akka` are a direct port of the decision procedure
  in `packages/basic-crawler/src/internals/basic-crawler.ts`,
  `packages/core/src/session_pool/session.ts` and `session_pool.ts`, and
  `packages/core/src/storages/throttling_request_manager.ts`. This is the nature of a port and
  is not something to obscure.
- Because no Apache-2.0 text was copied into this repository, nothing here is bound by
  apify/crawlee's licence terms — the "copied material carries its licence with it" rule does
  not trigger, since nothing was copied. `LICENSE-crawlee` carries the original licence text
  for reference and attribution only.

## Also used

- Akka
