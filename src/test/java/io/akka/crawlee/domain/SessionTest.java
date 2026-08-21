package io.akka.crawlee.domain;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SessionTest {

  @Test
  void newSessionIsUsable() {
    Session s = Session.create("s1", 50, 3, 0.5);
    assertTrue(s.isUsable());
  }

  @Test
  void markGoodDecrementsErrorScoreByDecrementOnly() {
    Session s = Session.create("s1", 50, 3, 0.5).markBad().markGood();
    // errorScore: 0 -> 1 (markBad) -> 0.5 (markGood)
    assertEquals(0.5, s.errorScore(), 1e-9);
    assertEquals(2, s.usageCount());
  }

  @Test
  void markGoodDoesNotDecrementBelowZero() {
    Session s = Session.create("s1", 50, 3, 0.5).markGood();
    assertEquals(0, s.errorScore(), 1e-9);
  }

  @Test
  void becomesBlockedAtMaxErrorScoreAndSelfRetires() {
    Session s = Session.create("s1", 50, 3, 0.5).markBad().markBad().markBad();
    assertTrue(s.isBlocked());
    assertTrue(s.retired());
    assertFalse(s.isUsable());
  }

  @Test
  void maxUsageCountReachedMakesSessionUnusableAndSelfRetires() {
    Session s = Session.create("s1", 2, 100, 0.5);
    s = s.markGood(); // usage 1
    s = s.markGood(); // usage 2 == max
    assertTrue(s.isMaxUsageCountReached());
    assertTrue(s.retired());
  }

  @Test
  void retireIsTerminalAndIdempotent() {
    Session s = Session.create("s1", 50, 3, 0.5).retire();
    Session again = s.retire();
    assertEquals(s, again);
    assertFalse(s.isUsable());
  }

  @Test
  void retireAddsMaxErrorScoreToErrorScore() {
    Session s = Session.create("s1", 50, 3, 0.5).retire();
    assertEquals(3, s.errorScore(), 1e-9);
    assertEquals(1, s.usageCount());
  }
}
