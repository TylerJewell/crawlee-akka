package io.akka.crawlee.domain;

/**
 * One rotation slot in the session pool — apify/crawlee's {@code Session},
 * packages/core/src/session_pool/session.ts.
 *
 * <p>Immutable: every mutator returns the next {@link Session} rather than mutating in place, so
 * an owning entity can persist the transition as an event. {@code errorScore}/{@code usageCount}
 * are {@code double}/{@code int} to match the source, which decrements {@code errorScore} by a
 * fractional {@code errorScoreDecrement} (session.ts:255-259) — that field cannot be an int
 * without changing when a session becomes usable again.
 */
public record Session(
    String id,
    double errorScore,
    int usageCount,
    int maxUsageCount,
    double maxErrorScore,
    double errorScoreDecrement,
    boolean retired) {

  public static Session create(String id, int maxUsageCount, double maxErrorScore, double errorScoreDecrement) {
    return new Session(id, 0, 0, maxUsageCount, maxErrorScore, errorScoreDecrement, false);
  }

  /** session.ts:223-225. */
  public boolean isBlocked() {
    return errorScore >= maxErrorScore;
  }

  /** session.ts:239-242. */
  public boolean isMaxUsageCountReached() {
    return usageCount >= maxUsageCount;
  }

  /** session.ts:247-249. Expiry (maxAgeSecs) is out of this slice — no wall-clock state here. */
  public boolean isUsable() {
    return !retired && !isBlocked() && !isMaxUsageCountReached();
  }

  /**
   * session.ts:253-262 ({@code markGood}), folded together with the self-retire check
   * (session.ts:325-329) that the source runs after every mutation.
   */
  public Session markGood() {
    double nextScore = errorScore > 0 ? errorScore - errorScoreDecrement : errorScore;
    Session next = new Session(id, nextScore, usageCount + 1, maxUsageCount, maxErrorScore, errorScoreDecrement, retired);
    return next.maybeSelfRetire();
  }

  /** session.ts:299-304, plus the same self-retire check as {@link #markGood()}. */
  public Session markBad() {
    Session next =
        new Session(id, errorScore + 1, usageCount + 1, maxUsageCount, maxErrorScore, errorScoreDecrement, retired);
    return next.maybeSelfRetire();
  }

  /**
   * session.ts:283-291. Retirement is terminal and idempotent — a second {@code retire()} is a
   * no-op in the source, which this mirrors by returning {@code this} unchanged.
   */
  public Session retire() {
    if (retired) {
      return this;
    }
    return new Session(id, errorScore + maxErrorScore, usageCount + 1, maxUsageCount, maxErrorScore, errorScoreDecrement, true);
  }

  private Session maybeSelfRetire() {
    return isUsable() ? this : retire();
  }
}
