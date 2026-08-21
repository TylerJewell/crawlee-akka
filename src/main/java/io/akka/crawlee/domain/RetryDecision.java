package io.akka.crawlee.domain;

/**
 * Whether a failed request should go back on the queue — apify/crawlee's
 * {@code BasicCrawler#canRequestBeRetried}, packages/basic-crawler/src/internals/basic-crawler.ts:2857-2869.
 *
 * <p>{@code noRetry} and a non-retryable error both force {@code false} regardless of the count
 * (basic-crawler.ts:2859-2861). A forced retry — the source's {@code RetryRequestError}, thrown
 * by request handler code that calls {@code crawler.addRequests}-style manual retry — bypasses the
 * count entirely (basic-crawler.ts:2863-2865). Otherwise the request may retry while
 * {@code retryCount < maxRetries} (basic-crawler.ts:2867-2868).
 */
public final class RetryDecision {

  private RetryDecision() {}

  public static boolean canRetry(
      int retryCount, int maxRetries, boolean noRetry, boolean isNonRetryableError, boolean isForcedRetry) {
    if (noRetry || isNonRetryableError) {
      return false;
    }
    if (isForcedRetry) {
      return true;
    }
    return retryCount < maxRetries;
  }
}
