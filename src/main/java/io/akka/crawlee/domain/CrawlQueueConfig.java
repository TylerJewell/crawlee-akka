package io.akka.crawlee.domain;

/** Tunables carried over from {@code BasicCrawlerOptions}, {@code SessionPoolOptions} and
 * {@code ThrottlingRequestManagerOptions}, defaults matching the source. */
public record CrawlQueueConfig(
    int maxRequestRetries,
    int maxPoolSize,
    SessionReuseStrategy sessionReuseStrategy,
    int sessionMaxUsageCount,
    double sessionMaxErrorScore,
    double sessionErrorScoreDecrement,
    long baseDelayMs,
    long maxDelayMs,
    long minCrawlDelayMs,
    long maxDomainStallMs) {

  public static CrawlQueueConfig defaults() {
    return new CrawlQueueConfig(3, 1000, SessionReuseStrategy.RANDOM, 50, 3, 0.5, 2000, 60_000, 0, 900_000);
  }
}
