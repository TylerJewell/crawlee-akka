package io.akka.crawlee.domain;

import akka.javasdk.annotations.TypeName;

public sealed interface CrawlQueueEvent {

  @TypeName("request-enqueued")
  record RequestEnqueued(PendingRequest request) implements CrawlQueueEvent {}

  /** A dispatchable domain's crawl-delay clock is armed ahead of the fetch it accompanies —
   * throttling_request_manager.ts:920-928. */
  @TypeName("crawl-delay-armed")
  record CrawlDelayArmed(String domain, long crawlDelayUntilMs) implements CrawlQueueEvent {}

  @TypeName("request-dispatched")
  record RequestDispatched(String requestId, String domain, String sessionId) implements CrawlQueueEvent {}

  @TypeName("session-created")
  record SessionCreated(Session session) implements CrawlQueueEvent {}

  @TypeName("session-updated")
  record SessionUpdated(Session session) implements CrawlQueueEvent {}

  @TypeName("round-robin-advanced")
  record RoundRobinAdvanced(int nextIndex) implements CrawlQueueEvent {}

  @TypeName("request-succeeded")
  record RequestSucceeded(String requestId) implements CrawlQueueEvent {}

  @TypeName("request-requeued")
  record RequestRequeued(PendingRequest request) implements CrawlQueueEvent {}

  @TypeName("request-failed-terminally")
  record RequestFailedTerminally(String requestId) implements CrawlQueueEvent {}

  @TypeName("domain-backoff-recorded")
  record DomainBackoffRecorded(DomainThrottle throttle) implements CrawlQueueEvent {}

  @TypeName("domain-progress-recorded")
  record DomainProgressRecorded(String domain) implements CrawlQueueEvent {}

  /** Emitted only the first time a domain declares a crawl delay — throttling_request_manager.ts:628-644's
   * "first value wins" is decided before this event is created, not when it is applied. */
  @TypeName("domain-crawl-delay-declared")
  record DomainCrawlDelayDeclared(String domain, long declaredCrawlDelayMs) implements CrawlQueueEvent {}
}
