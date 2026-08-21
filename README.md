# crawlee-akka

Decides, for a stream of URLs to fetch, what gets dispatched next, which rotating
session carries it, and how long a domain that has rate-limited or asked for a
crawl delay must be left alone — the crawl queue's retry, session rotation and
politeness logic from apify/crawlee.

A port of [apify/crawlee](https://github.com/apify/crawlee) onto **Akka**, built with
**Akka Specify**.

---

## Where it came from

apify/crawlee is a web crawling and browser automation library. It was ported to derive
a specification format precise enough to regenerate a system on a different stack — the
port is the vehicle, the specification is the deliverable.

Only one part of crawlee is rebuilt here: the crawl queue's retry decision, session
rotation, and per-domain politeness scheduling — not URL storage, deduplication, or the
actual page fetch. The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness)
under `crawlee-port/`.

---

## apify/crawlee → this port

📉 984 TypeScript lines (session rotation, politeness, and the retry decision) → **540 Java lines**<br>
📁 4 files → **11 files**<br>
⚡ 6.5 nanoseconds → **1.2 nanoseconds** to decide whether a failed request can be retried<br>
🧪 0 tests → **40 tests, 5 mutation probes, all killed by the intended test**<br>
🔌 a library called in-process → **an HTTP surface, one crawl run per entity**

Full method and the numbers that did *not* make this list: [`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/crawlee-port/bench/REPORT.md).

---

## What it took to build

⏱️ **0.5 hours** from the first command to the published repository, **0.5** of them active<br>
💬 **347** exchanges with the model<br>
✍️ **185,734** tokens written by the model, **62,750,741** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **40** tests

```bash
python toolkit/tokens.py --port crawlee    # turns, tokens, elapsed and active time
```

The record of every question, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

From the specification:

- **A failed request that is not forbidden from retrying, and has not used up its
  budget, goes back on the queue.** A request marked "never retry," or one whose error
  says retrying cannot help, is dropped instead — no matter how many attempts it has
  left.
- **A session that goes bad enough stops being handed out, and a poisoned one never
  comes back.** Every use scores a session a little worse or a little better; enough bad
  scores, or too many total uses, retires it for good.
- **A domain that has been rate-limited waits longer each time it happens again, up to a
  ceiling — unless it has been quiet for a while, in which case the wait starts over.**
  A domain that asked for a specific crawl delay gets at least that much space between
  requests, whichever asked for more.

Generated documentation lives at [`docs/index.html`](docs/index.html) — open it in a
browser for the entity diagram, the interaction path, and the component reference.

---

## Design decisions

**One entity per crawl run.** A session pool and a set of per-domain clocks only make
sense together — picking a session and deciding whether a domain is free to be dispatched
to happen at the same moment, for the same request. Keeping all three in one entity means
that moment is always decided from a single, consistent view, instead of two components
that might disagree about what just happened.

**Randomness and the clock are arguments, never read from inside.** Every rule in this
port is a function that takes "what time is it" and "which random session was picked" as
plain numbers instead of asking the computer directly. That means a decision, once made,
can be written down and replayed exactly — which is what lets the system recover after a
restart without guessing what it would have decided differently the second time.

**Domains are throttled the moment they misbehave, not the moment they are first seen.**
The original starts tracking every domain's pace as soon as its first request shows up.
This port waits until a domain actually rate-limits or asks for a specific delay before
it starts tracking it, which keeps a crawl that never hits trouble simpler to reason
about at the cost of a small ordering difference documented below.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/crawlee-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Call it** at http://localhost:9044.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9044**.

### Try it

```bash
curl -X POST localhost:9044/runs/run-1/requests \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.test/1","domain":"example.test"}'

curl -X POST localhost:9044/runs/run-1/dispatch

curl localhost:9044/runs/run-1
```

---

## Configuration

Everything in this port is set through `CrawlQueueConfig.defaults()` — there is no
environment-variable configuration surface in this slice. See
`src/main/java/io/akka/crawlee/domain/CrawlQueueConfig.java` for every default and where
it was read from.

---

## Where it differs from apify/crawlee

Everything not listed here behaves the same way on purpose, including the parts that
look like mistakes.

- **When a domain starts being paced.** apify/crawlee begins tracking every domain's
  crawl delay and backoff clocks the moment its first request is added to the queue, in
  the mode this port matches (throttle every domain). This port instead starts tracking a
  domain only the first time it rate-limits or declares a crawl delay — before that, its
  requests dispatch immediately, the same as apify/crawlee's own behaviour when no delay
  has been set. The two agree once every domain in play has actually been throttled at
  least once; they can disagree on which of several never-throttled domains goes first,
  because this port has no per-domain clock yet to sort them by.
- **How long retired sessions stay in memory.** apify/crawlee only sweeps retired
  sessions out of the pool once the pool is full. This port does the same — sessions are
  never swept early — so a very long crawl run that retires far more sessions than the
  pool's maximum size, without the pool itself ever filling, keeps every retired session
  in memory. Not checked against apify/crawlee at that scale; both sides share the same
  behaviour, and whether either holds up under it is `not checked`.
- **Session expiry by age is not implemented.** apify/crawlee retires a session once it is
  older than a configured age, in addition to the checks this port implements (error
  score, usage count, explicit retirement). This port's slice was scoped to retries,
  rotation and politeness — age-based expiry depends only on wall-clock duration, not on
  any of those three, and was left out rather than guessed at.
- **Request queue storage is not implemented.** apify/crawlee's request queue also
  deduplicates by URL and supports priority ordering and persistence; this port holds
  pending requests in a plain per-domain list scoped to one crawl run's lifetime.

---

## Licence

apify/crawlee is Apache License 2.0, © 2018 Apify Technologies s.r.o. This port
reimplements the behaviour without copied source; see `ACKNOWLEDGEMENTS.md`.
