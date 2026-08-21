package io.akka.crawlee.application;

import akka.Done;
import akka.javasdk.annotations.Component;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import akka.javasdk.eventsourcedentity.EventSourcedEntityContext;
import io.akka.crawlee.domain.CrawlQueueConfig;
import io.akka.crawlee.domain.CrawlQueueEvent;
import io.akka.crawlee.domain.CrawlQueueState;
import io.akka.crawlee.domain.PendingRequest;
import io.akka.crawlee.domain.Session;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * One crawl run's queue — SPEC-001. The entity id is the crawl run id. All decision logic lives
 * in {@code io.akka.crawlee.domain} and is tested without a runtime; this class decides only what
 * to persist, and supplies the wall-clock time and randomness the pure functions take as
 * parameters.
 */
@Component(id = "crawl-queue")
public class CrawlQueueEntity extends EventSourcedEntity<CrawlQueueState, CrawlQueueEvent> {

  public CrawlQueueEntity(EventSourcedEntityContext context) {}

  @Override
  public CrawlQueueState emptyState() {
    return CrawlQueueState.empty(CrawlQueueConfig.defaults());
  }

  public record EnqueueCommand(String url, String domain) {}

  public Effect<String> enqueue(EnqueueCommand cmd) {
    String requestId = UUID.randomUUID().toString();
    return effects()
        .persist(new CrawlQueueEvent.RequestEnqueued(new PendingRequest(requestId, cmd.url(), cmd.domain(), 0)))
        .thenReply(state -> requestId);
  }

  public record DispatchedRequest(String requestId, String url, String domain, String sessionId) {}

  public Effect<Optional<DispatchedRequest>> fetchNext() {
    long now = Instant.now().toEpochMilli();
    int randomSessionIndex =
        currentState().sessionPool().sessions().isEmpty()
            ? 0
            : ThreadLocalRandom.current().nextInt(currentState().sessionPool().sessions().size());
    String newSessionId = "session-" + UUID.randomUUID();

    Optional<CrawlQueueState.DispatchPlan> plan = currentState().planDispatch(now, randomSessionIndex, newSessionId);
    if (plan.isEmpty()) {
      return effects().reply(Optional.empty());
    }

    var p = plan.get();
    return effects()
        .persistAll(p.events())
        .thenReply(
            state -> Optional.of(new DispatchedRequest(p.request().id(), p.request().url(), p.request().domain(), p.session().id())));
  }

  public Effect<Done> reportSuccess(String requestId) {
    List<CrawlQueueEvent> events = currentState().planSuccess(requestId);
    if (events.isEmpty()) {
      return effects().error("request '" + requestId + "' is not in flight");
    }
    return effects().persistAll(events).thenReply(state -> Done.getInstance());
  }

  public record FailureReport(
      String requestId, boolean isSessionError, boolean isNonRetryableError, boolean isForcedRetry, Long retryAfterMs) {}

  public Effect<Done> reportFailure(FailureReport report) {
    long now = Instant.now().toEpochMilli();
    List<CrawlQueueEvent> events =
        currentState()
            .planFailure(
                report.requestId(), now, report.isSessionError(), report.isNonRetryableError(), report.isForcedRetry(),
                report.retryAfterMs());
    if (events.isEmpty()) {
      return effects().error("request '" + report.requestId() + "' is not in flight");
    }
    return effects().persistAll(events).thenReply(state -> Done.getInstance());
  }

  public record DeclareCrawlDelay(String domain, long delaySeconds) {}

  public Effect<Done> declareCrawlDelay(DeclareCrawlDelay cmd) {
    List<CrawlQueueEvent> events = currentState().planCrawlDelayDeclaration(cmd.domain(), cmd.delaySeconds());
    if (events.isEmpty()) {
      return effects().reply(Done.getInstance());
    }
    return effects().persistAll(events).thenReply(state -> Done.getInstance());
  }

  public ReadOnlyEffect<CrawlQueueState> get() {
    return effects().reply(currentState());
  }

  public ReadOnlyEffect<Optional<Session>> getSession(String sessionId) {
    return effects().reply(currentState().sessionPool().byId(sessionId));
  }

  @Override
  public CrawlQueueState applyEvent(CrawlQueueEvent event) {
    return currentState().apply(event);
  }
}
