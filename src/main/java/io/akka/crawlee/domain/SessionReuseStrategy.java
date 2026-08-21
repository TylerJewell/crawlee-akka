package io.akka.crawlee.domain;

/** session_pool.ts:81-84. */
public enum SessionReuseStrategy {
  RANDOM,
  ROUND_ROBIN,
  USE_UNTIL_FAILURE
}
