package io.akka.crawlee.domain;

/**
 * Per-domain politeness clocks — apify/crawlee's {@code ThrottlingRequestManager} domain state,
 * packages/core/src/storages/throttling_request_manager.ts:178-224.
 *
 * <p>Two independent clocks, both expressed as epoch milliseconds the caller supplies (never read
 * from a wall clock in here, so the whole class stays a pure function of its inputs):
 *
 * <ul>
 *   <li>{@code backoffUntil} — reactive, set by a 429, doubling from a base delay and capped
 *       (throttling_request_manager.ts:561-614).
 *   <li>{@code crawlDelayUntil} — proactive, armed on every dispatch to the floor of the domain's
 *       declared {@code Crawl-delay} and the configured minimum
 *       (throttling_request_manager.ts:207-209).
 * </ul>
 */
public record DomainThrottle(
    String domain,
    long backoffUntilMs,
    long crawlDelayUntilMs,
    long backoffDecaysAtMs,
    int consecutive429Count,
    Long declaredCrawlDelayMs,
    long rateLimitedSinceMs,
    long lastRateLimitedAtMs) {

  public static DomainThrottle create(String domain) {
    return new DomainThrottle(domain, 0, 0, 0, 0, null, 0, 0);
  }

  /** throttling_request_manager.ts:204-206. */
  public long throttledUntil() {
    return Math.max(backoffUntilMs, crawlDelayUntilMs);
  }

  /** throttling_request_manager.ts:209-211. */
  public long crawlDelayMillis(long minCrawlDelayMs) {
    return Math.max(declaredCrawlDelayMs == null ? 0 : declaredCrawlDelayMs, minCrawlDelayMs);
  }

  public boolean isDispatchable(long now) {
    return now >= throttledUntil();
  }

  /**
   * throttling_request_manager.ts:916-935: arms the crawl-delay clock for the next dispatch,
   * ahead of actually fetching a request for the domain, so a concurrent dispatch cannot land in
   * the same window.
   */
  public DomainThrottle armCrawlDelay(long now, long minCrawlDelayMs) {
    long delay = crawlDelayMillis(minCrawlDelayMs);
    if (delay <= 0) {
      return this;
    }
    return withCrawlDelayUntil(now + delay);
  }

  /** Applies a crawl-delay-until value already decided by the caller — used to replay an event. */
  public DomainThrottle withCrawlDelayUntil(long crawlDelayUntilMs) {
    return new DomainThrottle(
        domain, backoffUntilMs, crawlDelayUntilMs, backoffDecaysAtMs, consecutive429Count, declaredCrawlDelayMs,
        rateLimitedSinceMs, lastRateLimitedAtMs);
  }

  /**
   * throttling_request_manager.ts:561-614. {@code retryAfterMs} is {@code null} when the response
   * carried no {@code Retry-After} header, in which case the delay is {@code baseDelayMs * 2^(n-1)}.
   */
  public DomainThrottle recordDelay(long now, Long retryAfterMs, long baseDelayMs, long maxDelayMs) {
    long lastRateLimitedAt = now;
    long rateLimitedSince = rateLimitedSinceMs == 0 ? now : rateLimitedSinceMs;

    // A 429 for a request already in flight when the limit first hit is one rate-limit event, not
    // a new one — only the first one past the current backoff window advances the exponent.
    if (now < backoffUntilMs) {
      return new DomainThrottle(
          domain, backoffUntilMs, crawlDelayUntilMs, backoffDecaysAtMs, consecutive429Count, declaredCrawlDelayMs,
          rateLimitedSince, lastRateLimitedAt);
    }

    int nextCount = (now >= backoffDecaysAtMs ? 0 : consecutive429Count) + 1;

    long delayMs = retryAfterMs != null ? retryAfterMs : baseDelayMs * (1L << (nextCount - 1));
    if (delayMs > maxDelayMs) {
      delayMs = maxDelayMs;
    }

    long backoffUntil = now + delayMs;
    long backoffDecaysAt = backoffUntil + delayMs;

    return new DomainThrottle(
        domain, backoffUntil, crawlDelayUntilMs, backoffDecaysAt, nextCount, declaredCrawlDelayMs,
        rateLimitedSince, lastRateLimitedAt);
  }

  /**
   * throttling_request_manager.ts:628-644. The first declared value wins, so a robots.txt re-fetch
   * mid-crawl cannot change the cadence.
   */
  public DomainThrottle withDeclaredCrawlDelay(long delaySeconds) {
    if (declaredCrawlDelayMs != null) {
      return this;
    }
    return withDeclaredCrawlDelayMs(delaySeconds * 1000);
  }

  /** Applies a declared-crawl-delay value already decided by the caller — used to replay an event. */
  public DomainThrottle withDeclaredCrawlDelayMs(long declaredCrawlDelayMs) {
    return new DomainThrottle(
        domain, backoffUntilMs, crawlDelayUntilMs, backoffDecaysAtMs, consecutive429Count, declaredCrawlDelayMs,
        rateLimitedSinceMs, lastRateLimitedAtMs);
  }

  /**
   * throttling_request_manager.ts:695-701 ({@code #recordProgress}): a dispatch that got through
   * ends the stall-detection window.
   */
  public DomainThrottle recordProgress() {
    if (rateLimitedSinceMs == 0) {
      return this;
    }
    return new DomainThrottle(
        domain, backoffUntilMs, crawlDelayUntilMs, backoffDecaysAtMs, consecutive429Count, declaredCrawlDelayMs, 0,
        lastRateLimitedAtMs);
  }

  /**
   * throttling_request_manager.ts:648-680 ({@code assertNoStalledDomains}): a domain qualifies as
   * stalled only while it is still being turned away *and* the run of 429s has gone unbroken past
   * the window — a domain that has simply gone quiet, or one that only just started failing,
   * does not count.
   */
  public boolean isStalled(long now, long maxDomainStallMs) {
    return rateLimitedSinceMs != 0
        && now - lastRateLimitedAtMs <= maxDomainStallMs
        && now - rateLimitedSinceMs > maxDomainStallMs;
  }
}
