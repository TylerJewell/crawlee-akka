package io.akka.crawlee.domain;

/** A request waiting to be dispatched. Scope note: the source's URL-based dedup and priority
 * queueing (RequestQueue proper) sit outside this slice — this is a plain FIFO per domain. */
public record PendingRequest(String id, String url, String domain, int retryCount) {

  public PendingRequest withIncrementedRetry() {
    return new PendingRequest(id, url, domain, retryCount + 1);
  }
}
