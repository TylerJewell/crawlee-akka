package io.akka.crawlee.domain;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RetryDecisionTest {

  @Test
  void noRetryFlagAlwaysWins() {
    assertFalse(RetryDecision.canRetry(0, 3, true, false, true));
  }

  @Test
  void nonRetryableErrorAlwaysWins() {
    assertFalse(RetryDecision.canRetry(0, 3, false, true, true));
  }

  @Test
  void forcedRetryIgnoresTheCount() {
    assertTrue(RetryDecision.canRetry(99, 3, false, false, true));
  }

  @Test
  void ordinaryRetryChecksCountAgainstMax() {
    assertTrue(RetryDecision.canRetry(2, 3, false, false, false));
    assertFalse(RetryDecision.canRetry(3, 3, false, false, false));
  }
}
