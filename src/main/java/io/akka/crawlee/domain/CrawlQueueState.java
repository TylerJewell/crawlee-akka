package io.akka.crawlee.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The crawl queue's full state and decision procedure: pending requests, the session pool, and
 * one {@link DomainThrottle} per domain seen. Ports the interaction between apify/crawlee's
 * {@code BasicCrawler} (retries), {@code SessionPool} (rotation) and
 * {@code ThrottlingRequestManager} (politeness) — the three pieces this slice covers.
 *
 * <p>Every method here is a pure function of its arguments; wall-clock time and randomness are
 * always parameters, never read internally, so the owning entity can persist a decision made once
 * and reapply it identically on replay.
 */
public record CrawlQueueState(
    CrawlQueueConfig config,
    List<PendingRequest> queue,
    Map<String, InFlight> inFlight,
    SessionPool sessionPool,
    Map<String, DomainThrottle> throttles) {

  public record InFlight(String requestId, String domain, String sessionId, int retryCount, String url) {}

  public static CrawlQueueState empty(CrawlQueueConfig config) {
    return new CrawlQueueState(
        config, List.of(), Map.of(), SessionPool.empty(config.maxPoolSize(), config.sessionReuseStrategy()), Map.of());
  }

  // -- pure decisions, called by the entity before it persists ---------------------------------

  /** The plan for one {@code fetchNext} call, or empty if nothing is currently dispatchable. */
  public record DispatchPlan(List<CrawlQueueEvent> events, PendingRequest request, Session session) {}

  /**
   * throttling_request_manager.ts:916-941 ({@code fetchNextRequest}), folded together with
   * session_pool.ts:309-333 ({@code getSession}): the domain that has been waiting longest and is
   * past both its clocks is served next, paired with whichever session the pool hands out for it.
   *
   * @param randomSessionIndex used only when the pool's strategy is {@code RANDOM} and a pick from
   *     the existing pool is possible; ignored otherwise.
   */
  public Optional<DispatchPlan> planDispatch(long now, int randomSessionIndex, String newSessionId) {
    List<String> fetchableDomains = fetchableDomainsOldestFirst(now);

    for (String domain : fetchableDomains) {
      Optional<PendingRequest> next = queue.stream().filter(r -> r.domain().equals(domain)).findFirst();
      if (next.isEmpty()) {
        continue;
      }
      return Optional.of(planFor(next.get(), now, randomSessionIndex, newSessionId));
    }

    // No throttled domain is both fetchable and has work; fall back to the first request whose
    // domain has no throttle state yet (never seen a 429 or a declared crawl delay).
    Optional<PendingRequest> untracked = queue.stream().filter(r -> !throttles.containsKey(r.domain())).findFirst();
    return untracked.map(r -> planFor(r, now, randomSessionIndex, newSessionId));
  }

  private List<String> fetchableDomainsOldestFirst(long now) {
    return throttles.values().stream()
        .filter(t -> t.isDispatchable(now))
        .sorted((a, b) -> Long.compare(a.throttledUntil(), b.throttledUntil()))
        .map(DomainThrottle::domain)
        .toList();
  }

  private DispatchPlan planFor(PendingRequest request, long now, int randomSessionIndex, String newSessionId) {
    List<CrawlQueueEvent> events = new ArrayList<>();

    DomainThrottle throttle = throttles.getOrDefault(request.domain(), DomainThrottle.create(request.domain()));
    long delay = throttle.crawlDelayMillis(config.minCrawlDelayMs());
    if (delay > 0) {
      events.add(new CrawlQueueEvent.CrawlDelayArmed(request.domain(), now + delay));
    }

    SessionPool.Pick pick = sessionPool.pick(randomSessionIndex);
    Session session;
    if (pick.session().isPresent()) {
      session = pick.session().get();
      if (config.sessionReuseStrategy() == SessionReuseStrategy.ROUND_ROBIN) {
        events.add(new CrawlQueueEvent.RoundRobinAdvanced(pick.nextPool().roundRobinIndex()));
      }
    } else {
      session =
          Session.create(newSessionId, config.sessionMaxUsageCount(), config.sessionMaxErrorScore(), config.sessionErrorScoreDecrement());
      events.add(new CrawlQueueEvent.SessionCreated(session));
    }

    events.add(new CrawlQueueEvent.RequestDispatched(request.id(), request.domain(), session.id()));

    return new DispatchPlan(events, request, session);
  }

  /**
   * basic-crawler.ts:2857-2869 and 2762-2777: a retryable failure goes back on the queue with
   * {@code retryCount} incremented and its session marked bad (or retired, for a session error);
   * a non-retryable one is dropped and the session is still charged, matching
   * basic-crawler.ts:2637-2639 and 2782-2784 where {@code markBad}/{@code retire} run regardless
   * of whether the request itself gets another try.
   */
  public List<CrawlQueueEvent> planFailure(
      String requestId,
      long now,
      boolean isSessionError,
      boolean isNonRetryableError,
      boolean isForcedRetry,
      Long retryAfterMs) {
    InFlight flight = inFlight.get(requestId);
    if (flight == null) {
      return List.of();
    }

    List<CrawlQueueEvent> events = new ArrayList<>();

    Optional<Session> maybeSession = sessionPool.byId(flight.sessionId());
    maybeSession.ifPresent(
        session -> events.add(new CrawlQueueEvent.SessionUpdated(isSessionError ? session.retire() : session.markBad())));

    if (retryAfterMs != null || isSessionError) {
      DomainThrottle throttle = throttles.getOrDefault(flight.domain(), DomainThrottle.create(flight.domain()));
      events.add(
          new CrawlQueueEvent.DomainBackoffRecorded(
              throttle.recordDelay(now, retryAfterMs, config.baseDelayMs(), config.maxDelayMs())));
    }

    boolean canRetry =
        RetryDecision.canRetry(
            flight.retryCount(), config.maxRequestRetries(), false, isNonRetryableError, isForcedRetry || isSessionError);
    if (canRetry) {
      events.add(
          new CrawlQueueEvent.RequestRequeued(
              new PendingRequest(flight.requestId(), flight.url(), flight.domain(), flight.retryCount() + 1)));
    } else {
      events.add(new CrawlQueueEvent.RequestFailedTerminally(requestId));
    }

    return events;
  }

  /** basic-crawler.ts:2762-2764: a session error retires the session but never touches the request's own
   * retry count directly — the count still advances by one, same as any other retry, via {@code planFailure}. */
  public List<CrawlQueueEvent> planSuccess(String requestId) {
    InFlight flight = inFlight.get(requestId);
    if (flight == null) {
      return List.of();
    }
    List<CrawlQueueEvent> events = new ArrayList<>();
    sessionPool.byId(flight.sessionId()).ifPresent(session -> events.add(new CrawlQueueEvent.SessionUpdated(session.markGood())));
    events.add(new CrawlQueueEvent.DomainProgressRecorded(flight.domain()));
    events.add(new CrawlQueueEvent.RequestSucceeded(requestId));
    return events;
  }

  /** throttling_request_manager.ts:628-644: a no-op event when the domain already has a declared delay. */
  public List<CrawlQueueEvent> planCrawlDelayDeclaration(String domain, long delaySeconds) {
    DomainThrottle throttle = throttles.getOrDefault(domain, DomainThrottle.create(domain));
    if (throttle.declaredCrawlDelayMs() != null) {
      return List.of();
    }
    return List.of(new CrawlQueueEvent.DomainCrawlDelayDeclared(domain, delaySeconds * 1000));
  }

  // -- pure application of events, mirrors the entity's applyEvent -----------------------------

  public CrawlQueueState apply(CrawlQueueEvent event) {
    return switch (event) {
      case CrawlQueueEvent.RequestEnqueued e -> withQueue(append(queue, e.request()));
      case CrawlQueueEvent.CrawlDelayArmed e -> withThrottle(e.domain(), t -> t.withCrawlDelayUntil(e.crawlDelayUntilMs()));
      case CrawlQueueEvent.RequestDispatched e -> dispatch(e);
      case CrawlQueueEvent.SessionCreated e -> withSessionPool(sessionPool.withSession(e.session()));
      case CrawlQueueEvent.SessionUpdated e -> withSessionPool(sessionPool.replace(e.session()));
      case CrawlQueueEvent.RoundRobinAdvanced e -> withSessionPool(
          new SessionPool(sessionPool.sessions(), sessionPool.maxPoolSize(), sessionPool.strategy(), e.nextIndex()));
      case CrawlQueueEvent.RequestSucceeded e -> withInFlight(removeFlight(e.requestId()));
      case CrawlQueueEvent.RequestRequeued e -> requeue(e.request());
      case CrawlQueueEvent.RequestFailedTerminally e -> withInFlight(removeFlight(e.requestId()));
      case CrawlQueueEvent.DomainBackoffRecorded e -> withThrottleValue(e.throttle());
      case CrawlQueueEvent.DomainProgressRecorded e -> withThrottle(e.domain(), DomainThrottle::recordProgress);
      case CrawlQueueEvent.DomainCrawlDelayDeclared e -> withThrottle(e.domain(), t -> t.withDeclaredCrawlDelayMs(e.declaredCrawlDelayMs()));
    };
  }

  private CrawlQueueState requeue(PendingRequest request) {
    Map<String, InFlight> nextFlight = removeFlight(request.id());
    return new CrawlQueueState(config, append(queue, request), nextFlight, sessionPool, throttles);
  }

  private CrawlQueueState dispatch(CrawlQueueEvent.RequestDispatched e) {
    PendingRequest req = queue.stream().filter(r -> r.id().equals(e.requestId())).findFirst().orElseThrow();
    List<PendingRequest> nextQueue = queue.stream().filter(r -> !r.id().equals(e.requestId())).toList();
    Map<String, InFlight> nextFlight = new TreeMap<>(inFlight);
    nextFlight.put(e.requestId(), new InFlight(e.requestId(), e.domain(), e.sessionId(), req.retryCount(), req.url()));
    return new CrawlQueueState(config, nextQueue, Map.copyOf(nextFlight), sessionPool, throttles);
  }

  private Map<String, InFlight> removeFlight(String requestId) {
    Map<String, InFlight> next = new TreeMap<>(inFlight);
    next.remove(requestId);
    return Map.copyOf(next);
  }

  private static List<PendingRequest> append(List<PendingRequest> list, PendingRequest r) {
    List<PendingRequest> next = new ArrayList<>(list);
    next.add(r);
    return List.copyOf(next);
  }

  private CrawlQueueState withQueue(List<PendingRequest> q) {
    return new CrawlQueueState(config, q, inFlight, sessionPool, throttles);
  }

  private CrawlQueueState withInFlight(Map<String, InFlight> f) {
    return new CrawlQueueState(config, queue, f, sessionPool, throttles);
  }

  private CrawlQueueState withSessionPool(SessionPool p) {
    return new CrawlQueueState(config, queue, inFlight, p, throttles);
  }

  private CrawlQueueState withThrottleValue(DomainThrottle t) {
    Map<String, DomainThrottle> next = new TreeMap<>(throttles);
    next.put(t.domain(), t);
    return new CrawlQueueState(config, queue, inFlight, sessionPool, Map.copyOf(next));
  }

  private CrawlQueueState withThrottle(String domain, java.util.function.UnaryOperator<DomainThrottle> f) {
    DomainThrottle current = throttles.getOrDefault(domain, DomainThrottle.create(domain));
    return withThrottleValue(f.apply(current));
  }
}
