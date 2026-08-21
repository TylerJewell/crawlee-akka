package io.akka.crawlee.domain;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class SessionPoolTest {

  @Test
  void picksNothingWhileThereIsSpaceForANewSession() {
    SessionPool pool = SessionPool.empty(2, SessionReuseStrategy.RANDOM);
    SessionPool.Pick pick = pool.pick(0);
    assertTrue(pick.session().isEmpty());
  }

  @Test
  void useUntilFailurePicksRegardlessOfSpace() {
    Session s = Session.create("s1", 50, 3, 0.5);
    SessionPool pool = SessionPool.empty(5, SessionReuseStrategy.USE_UNTIL_FAILURE).withSession(s);
    SessionPool.Pick pick = pool.pick(0);
    assertEquals(Optional.of(s), pick.session());
  }

  @Test
  void useUntilFailureSkipsUnusableSessionsToFindAUsableOne() {
    Session bad = Session.create("bad", 50, 3, 0.5).retire();
    Session good = Session.create("good", 50, 3, 0.5);
    SessionPool pool =
        SessionPool.empty(5, SessionReuseStrategy.USE_UNTIL_FAILURE).withSession(bad).withSession(good);
    assertEquals(Optional.of(good), pool.pick(0).session());
  }

  @Test
  void roundRobinCyclesThroughSessionsInOrderAndAdvancesEvenWhenUnusable() {
    Session a = Session.create("a", 50, 3, 0.5);
    Session b = Session.create("b", 50, 3, 0.5).retire();
    SessionPool full = SessionPool.empty(2, SessionReuseStrategy.ROUND_ROBIN).withSession(a).withSession(b);

    SessionPool.Pick first = full.pick(0);
    assertEquals(Optional.of(a), first.session());
    assertEquals(1, first.nextPool().roundRobinIndex());

    SessionPool.Pick second = first.nextPool().pick(0);
    assertTrue(second.session().isEmpty()); // b is retired
    assertEquals(2, second.nextPool().roundRobinIndex());
  }

  @Test
  void randomPickReturnsEmptyWhenIndexedSessionIsUnusable() {
    Session retired = Session.create("r", 50, 3, 0.5).retire();
    SessionPool pool = SessionPool.empty(1, SessionReuseStrategy.RANDOM).withSession(retired);
    assertTrue(pool.pick(0).session().isEmpty());
  }

  @Test
  void withoutRetiredSessionsDropsUnusableOnes() {
    Session a = Session.create("a", 50, 3, 0.5);
    Session b = Session.create("b", 50, 3, 0.5).retire();
    SessionPool pool = SessionPool.empty(5, SessionReuseStrategy.RANDOM).withSession(a).withSession(b);
    SessionPool cleaned = pool.withoutRetiredSessions();
    assertEquals(1, cleaned.sessions().size());
    assertEquals("a", cleaned.sessions().get(0).id());
  }
}
