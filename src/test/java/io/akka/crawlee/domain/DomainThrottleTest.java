package io.akka.crawlee.domain;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class DomainThrottleTest {

  @Test
  void freshDomainIsImmediatelyDispatchable() {
    DomainThrottle t = DomainThrottle.create("example.com");
    assertTrue(t.isDispatchable(0));
  }

  @Test
  void recordDelayWithNoRetryAfterDoublesFromBaseDelay() {
    DomainThrottle t = DomainThrottle.create("example.com");
    long base = 2000, max = 60_000;
    t = t.recordDelay(0, null, base, max); // 1st: 2000ms * 2^0 = 2000
    assertEquals(2000, t.backoffUntilMs());
    t = t.recordDelay(2000, null, base, max); // 2nd: 2000 * 2^1 = 4000
    assertEquals(6000, t.backoffUntilMs());
  }

  @Test
  void retryAfterHeaderOverridesExponentialBackoff() {
    DomainThrottle t = DomainThrottle.create("example.com").recordDelay(0, 15_000L, 2000, 60_000);
    assertEquals(15_000, t.backoffUntilMs());
  }

  @Test
  void delayIsCappedAtMaxDelayMs() {
    DomainThrottle t = DomainThrottle.create("example.com").recordDelay(0, 999_000L, 2000, 60_000);
    assertEquals(60_000, t.backoffUntilMs());
  }

  @Test
  void a429DuringActiveBackoffDoesNotAdvanceTheExponent() {
    DomainThrottle t = DomainThrottle.create("example.com");
    t = t.recordDelay(0, null, 2000, 60_000); // backoffUntil=2000, count=1
    DomainThrottle suppressed = t.recordDelay(1000, null, 2000, 60_000); // now < backoffUntil
    assertEquals(t.backoffUntilMs(), suppressed.backoffUntilMs());
    assertEquals(1, suppressed.consecutive429Count());
  }

  @Test
  void exponentResetsAfterAFullDecayWindow() {
    DomainThrottle t = DomainThrottle.create("example.com").recordDelay(0, null, 2000, 60_000);
    // backoffUntil=2000, backoffDecaysAt=4000
    DomainThrottle next = t.recordDelay(5000, null, 2000, 60_000);
    // now(5000) >= backoffDecaysAt(4000) -> count resets to 0 then +1 = 1 -> delay 2000
    assertEquals(1, next.consecutive429Count());
    assertEquals(7000, next.backoffUntilMs());
  }

  @Test
  void declaredCrawlDelayFirstValueWins() {
    DomainThrottle t = DomainThrottle.create("example.com").withDeclaredCrawlDelay(10);
    DomainThrottle again = t.withDeclaredCrawlDelay(30);
    assertEquals(10_000, again.declaredCrawlDelayMs());
  }

  @Test
  void crawlDelayFloorsAtConfiguredMinimum() {
    DomainThrottle t = DomainThrottle.create("example.com").withDeclaredCrawlDelay(1);
    assertEquals(5000, t.crawlDelayMillis(5000));
  }

  @Test
  void throttledUntilIsTheLaterOfBothClocks() {
    DomainThrottle t =
        DomainThrottle.create("example.com").recordDelay(0, 3000L, 2000, 60_000).withCrawlDelayUntil(9000);
    assertEquals(9000, t.throttledUntil());
  }

  @Test
  void stalledOnlyWhenUnbrokenRunExceedsWindowAndStillRecent() {
    DomainThrottle t = DomainThrottle.create("example.com").recordDelay(100, 1000L, 2000, 60_000);
    assertFalse(t.isStalled(500_100, 900_000)); // not past window yet
    DomainThrottle stillFailing = t.recordDelay(1300, 1000L, 2000, 60_000); // lastRateLimitedAt moves
    assertTrue(stillFailing.isStalled(900_200, 900_000)); // still failing, past the window
    assertFalse(stillFailing.isStalled(1_950_000, 900_000)); // last failure too long ago (idle, not stalled)
  }

  @Test
  void progressClearsTheStallWindow() {
    DomainThrottle t = DomainThrottle.create("example.com").recordDelay(100, 1000L, 2000, 60_000).recordProgress();
    assertEquals(0, t.rateLimitedSinceMs());
  }
}
