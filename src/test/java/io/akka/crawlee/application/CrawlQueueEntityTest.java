package io.akka.crawlee.application;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.crawlee.domain.CrawlQueueEvent;
import io.akka.crawlee.domain.CrawlQueueState;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** What {@code CrawlQueueStateTest} cannot check: that commands against requests the entity never
 * saw are refused, and that a full enqueue-dispatch-succeed round trip persists and replies as
 * expected through the entity, not just the pure domain layer. */
public class CrawlQueueEntityTest {

  private EventSourcedTestKit<CrawlQueueState, CrawlQueueEvent, CrawlQueueEntity> queue() {
    return EventSourcedTestKit.of("run-1", CrawlQueueEntity::new);
  }

  @Test
  public void reportingSuccessForAnUnknownRequestIsRefused() {
    var kit = queue();
    var result = kit.method(CrawlQueueEntity::reportSuccess).invoke("nope");
    assertThat(result.isError()).isTrue();
    assertThat(result.didPersistEvents()).isFalse();
  }

  @Test
  public void reportingFailureForAnUnknownRequestIsRefused() {
    var kit = queue();
    var result =
        kit.method(CrawlQueueEntity::reportFailure)
            .invoke(new CrawlQueueEntity.FailureReport("nope", false, false, false, null));
    assertThat(result.isError()).isTrue();
    assertThat(result.didPersistEvents()).isFalse();
  }

  @Test
  public void fetchNextOnAnEmptyQueueReturnsEmpty() {
    var kit = queue();
    var result = kit.method(CrawlQueueEntity::fetchNext).invoke();
    assertThat(result.isError()).isFalse();
    assertThat(result.getReply()).isEmpty();
  }

  @Test
  public void enqueueDispatchSucceedRoundTrip() {
    var kit = queue();

    var requestId = kit.method(CrawlQueueEntity::enqueue).invoke(new CrawlQueueEntity.EnqueueCommand("https://a.com/1", "a.com"));
    assertThat(requestId.isError()).isFalse();

    var dispatched = kit.method(CrawlQueueEntity::fetchNext).invoke();
    assertThat(dispatched.isError()).isFalse();
    Optional<CrawlQueueEntity.DispatchedRequest> maybeReq = dispatched.getReply();
    assertThat(maybeReq).isPresent();
    var req = maybeReq.get();
    assertThat(req.requestId()).isEqualTo(requestId.getReply());

    var success = kit.method(CrawlQueueEntity::reportSuccess).invoke(req.requestId());
    assertThat(success.isError()).isFalse();

    var state = kit.method(CrawlQueueEntity::get).invoke().getReply();
    assertThat(state.inFlight()).isEmpty();
    assertThat(state.queue()).isEmpty();
    assertThat(state.sessionPool().byId(req.sessionId())).isPresent();
  }

  @Test
  public void declaringACrawlDelayTwiceKeepsTheFirstValue() {
    var kit = queue();
    kit.method(CrawlQueueEntity::declareCrawlDelay).invoke(new CrawlQueueEntity.DeclareCrawlDelay("a.com", 10));
    var second = kit.method(CrawlQueueEntity::declareCrawlDelay).invoke(new CrawlQueueEntity.DeclareCrawlDelay("a.com", 30));
    assertThat(second.didPersistEvents()).isFalse();

    var state = kit.method(CrawlQueueEntity::get).invoke().getReply();
    assertThat(state.throttles().get("a.com").declaredCrawlDelayMs()).isEqualTo(10_000);
  }
}
