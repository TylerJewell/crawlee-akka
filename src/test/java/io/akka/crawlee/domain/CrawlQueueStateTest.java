package io.akka.crawlee.domain;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class CrawlQueueStateTest {

  private CrawlQueueState apply(CrawlQueueState state, List<CrawlQueueEvent> events) {
    for (var e : events) {
      state = state.apply(e);
    }
    return state;
  }

  @Test
  void dispatchCreatesASessionWhenThePoolIsEmpty() {
    CrawlQueueState state = CrawlQueueState.empty(CrawlQueueConfig.defaults());
    state = apply(state, List.of(new CrawlQueueEvent.RequestEnqueued(new PendingRequest("r1", "https://a.com/1", "a.com", 0))));

    Optional<CrawlQueueState.DispatchPlan> plan = state.planDispatch(1000, 0, "s1");
    assertTrue(plan.isPresent());
    assertEquals("s1", plan.get().session().id());
    assertEquals("r1", plan.get().request().id());

    state = apply(state, plan.get().events());
    assertEquals(1, state.sessionPool().sessions().size());
    assertTrue(state.queue().isEmpty());
    assertTrue(state.inFlight().containsKey("r1"));
  }

  @Test
  void retryableFailureRequeuesWithIncrementedRetryCountAndMarksSessionBad() {
    CrawlQueueState state = CrawlQueueState.empty(CrawlQueueConfig.defaults());
    state = apply(state, List.of(new CrawlQueueEvent.RequestEnqueued(new PendingRequest("r1", "https://a.com/1", "a.com", 0))));
    var plan = state.planDispatch(1000, 0, "s1").orElseThrow();
    state = apply(state, plan.events());

    var failureEvents = state.planFailure("r1", 2000, false, false, false, null);
    state = apply(state, failureEvents);

    assertEquals(1, state.queue().size());
    assertEquals(1, state.queue().get(0).retryCount());
    assertFalse(state.inFlight().containsKey("r1"));
    Session session = state.sessionPool().byId("s1").orElseThrow();
    assertEquals(1, session.errorScore(), 1e-9);
  }

  @Test
  void terminalFailureAfterMaxRetriesDropsTheRequestAndRetiresNothingExtra() {
    CrawlQueueConfig config =
        new CrawlQueueConfig(1, 1000, SessionReuseStrategy.RANDOM, 50, 3, 0.5, 2000, 60_000, 0, 900_000);
    CrawlQueueState state = CrawlQueueState.empty(config);
    state = apply(state, List.of(new CrawlQueueEvent.RequestEnqueued(new PendingRequest("r1", "https://a.com/1", "a.com", 1))));

    var plan = state.planDispatch(1000, 0, "s1").orElseThrow();
    state = apply(state, plan.events());

    // maxRequestRetries=1, request already at retryCount=1 -> not retryable
    var failureEvents = state.planFailure("r1", 2000, false, false, false, null);
    state = apply(state, failureEvents);

    assertTrue(state.queue().isEmpty());
    assertFalse(state.inFlight().containsKey("r1"));
  }

  @Test
  void sessionErrorRetiresTheSessionAndForcesARetryRegardlessOfCount() {
    CrawlQueueConfig config =
        new CrawlQueueConfig(0, 1000, SessionReuseStrategy.RANDOM, 50, 3, 0.5, 2000, 60_000, 0, 900_000);
    CrawlQueueState state = CrawlQueueState.empty(config);
    state = apply(state, List.of(new CrawlQueueEvent.RequestEnqueued(new PendingRequest("r1", "https://a.com/1", "a.com", 5))));
    var plan = state.planDispatch(1000, 0, "s1").orElseThrow();
    state = apply(state, plan.events());

    var failureEvents = state.planFailure("r1", 2000, true, false, false, null);
    state = apply(state, failureEvents);

    assertEquals(1, state.queue().size()); // still requeued despite maxRequestRetries=0
    assertTrue(state.sessionPool().byId("s1").orElseThrow().retired());
  }

  @Test
  void successMarksSessionGoodAndClearsStallWindow() {
    CrawlQueueState state = CrawlQueueState.empty(CrawlQueueConfig.defaults());
    state = apply(state, List.of(new CrawlQueueEvent.RequestEnqueued(new PendingRequest("r1", "https://a.com/1", "a.com", 0))));
    var plan = state.planDispatch(1000, 0, "s1").orElseThrow();
    state = apply(state, plan.events());

    state = apply(state, state.planSuccess("r1"));

    assertTrue(state.inFlight().isEmpty());
    assertEquals(1, state.sessionPool().byId("s1").orElseThrow().usageCount());
  }

  @Test
  void a429WithRetryAfterBacksOffTheDomainWithoutTouchingRetryCountBeyondNormal() {
    CrawlQueueState state = CrawlQueueState.empty(CrawlQueueConfig.defaults());
    state = apply(state, List.of(new CrawlQueueEvent.RequestEnqueued(new PendingRequest("r1", "https://a.com/1", "a.com", 0))));
    var plan = state.planDispatch(1000, 0, "s1").orElseThrow();
    state = apply(state, plan.events());

    var failureEvents = state.planFailure("r1", 2000, false, false, false, 30_000L);
    state = apply(state, failureEvents);

    DomainThrottle throttle = state.throttles().get("a.com");
    assertEquals(32_000, throttle.backoffUntilMs()); // 2000 (now) + 30_000 (Retry-After)
    assertEquals(1, state.queue().get(0).retryCount());
  }

  @Test
  void secondDomainIsServedWhileFirstIsBackingOff() {
    CrawlQueueConfig config = CrawlQueueConfig.defaults();
    CrawlQueueState state = CrawlQueueState.empty(config);
    state =
        apply(
            state,
            List.of(
                new CrawlQueueEvent.RequestEnqueued(new PendingRequest("r1", "https://a.com/1", "a.com", 0)),
                new CrawlQueueEvent.RequestEnqueued(new PendingRequest("r2", "https://b.com/1", "b.com", 0))));

    // Dispatch + fail r1 so a.com enters backoff.
    var plan1 = state.planDispatch(1000, 0, "s1").orElseThrow();
    state = apply(state, plan1.events());
    state = apply(state, state.planFailure("r1", 1100, false, false, false, 60_000L));

    // b.com has no throttle state yet, so it is served via the "untracked domain" fallback.
    var plan2 = state.planDispatch(2000, 0, "s2").orElseThrow();
    assertEquals("r2", plan2.request().id());
  }
}
