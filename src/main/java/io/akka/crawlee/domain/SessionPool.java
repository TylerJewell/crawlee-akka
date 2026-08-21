package io.akka.crawlee.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Rotation over a bounded set of {@link Session}s — apify/crawlee's {@code SessionPool},
 * packages/core/src/session_pool/session_pool.ts.
 *
 * <p>Immutable, and deliberately takes randomness as a parameter rather than drawing it itself:
 * an Akka entity may only persist the outcome of a random choice, not make the choice again on
 * replay, so the caller (the entity's command handler) draws the index and this class stays a
 * pure function of it.
 */
public record SessionPool(List<Session> sessions, int maxPoolSize, SessionReuseStrategy strategy, int roundRobinIndex) {

  public static SessionPool empty(int maxPoolSize, SessionReuseStrategy strategy) {
    return new SessionPool(List.of(), maxPoolSize, strategy, 0);
  }

  /** session_pool.ts:479-481. */
  public boolean hasSpaceForSession() {
    return sessions.size() < maxPoolSize;
  }

  /**
   * session_pool.ts:483-500. {@code randomIndex} is used only for the {@code RANDOM} strategy and
   * must be in {@code [0, sessions.size())}; the caller draws it (session_pool.ts:429-431's
   * {@code getRandomIndex}).
   *
   * <p>Returns the picked session and the pool with {@code roundRobinIndex} advanced, if that
   * strategy consumed it — round-robin's index moves whether or not the picked session turns out
   * usable, exactly as source's {@code index + 1} assignment does unconditionally.
   */
  public Pick pick(int randomIndex) {
    if (strategy != SessionReuseStrategy.USE_UNTIL_FAILURE && hasSpaceForSession()) {
      return new Pick(Optional.empty(), this);
    }

    if (strategy == SessionReuseStrategy.USE_UNTIL_FAILURE) {
      return new Pick(sessions.stream().filter(Session::isUsable).findFirst(), this);
    }

    if (sessions.isEmpty()) {
      return new Pick(Optional.empty(), this);
    }

    Session picked;
    SessionPool nextPool = this;
    if (strategy == SessionReuseStrategy.ROUND_ROBIN) {
      int index = roundRobinIndex % sessions.size();
      picked = sessions.get(index);
      nextPool = new SessionPool(sessions, maxPoolSize, strategy, index + 1);
    } else {
      picked = sessions.get(randomIndex);
    }

    return new Pick(picked.isUsable() ? Optional.of(picked) : Optional.empty(), nextPool);
  }

  public record Pick(Optional<Session> session, SessionPool nextPool) {}

  /** session_pool.ts:415-423 ({@code registerSession}), called after a new session is created. */
  public SessionPool withSession(Session session) {
    List<Session> next = new ArrayList<>(sessions);
    next.add(session);
    return new SessionPool(next, maxPoolSize, strategy, roundRobinIndex);
  }

  /** session_pool.ts:406-414. Filters on {@code isUsable()}, the same test as pool membership. */
  public SessionPool withoutRetiredSessions() {
    List<Session> next = sessions.stream().filter(Session::isUsable).toList();
    return new SessionPool(next, maxPoolSize, strategy, roundRobinIndex);
  }

  /** Replaces a session by id, e.g. after {@code markGood}/{@code markBad}/{@code retire}. */
  public SessionPool replace(Session updated) {
    List<Session> next = sessions.stream().map(s -> s.id().equals(updated.id()) ? updated : s).toList();
    return new SessionPool(next, maxPoolSize, strategy, roundRobinIndex);
  }

  public Optional<Session> byId(String id) {
    return sessions.stream().filter(s -> s.id().equals(id)).findFirst();
  }
}
