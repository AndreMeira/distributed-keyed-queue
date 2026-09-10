package homelab.keyedqueue.domain.service.lock


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.service.lock.LockStore.Hold
import zio.*

import java.time.Instant


/**
 * A distributed lock over a substrate: per-name mutual exclusion, on a renewable lease, with a fence.
 *
 * The sibling of `QueueStore`, and its distilled core — both are a per-key exclusive lease with a fencing
 * token, but a lock carries no messages, no order and no backoff, so it needs far less. See
 * `docs/research/distributed-lock.md`.
 *
 * '''Says nothing about Redis.''' As with the queue, the port is what lets a second substrate exist: a
 * Postgres form would hold the lease in a row and could even use a session advisory lock
 * (`docs/research/zookeeper-ideas.md`).
 *
 * '''No sweep in the contract.''' Reclaiming a dead holder's lock is not a background operation here — a
 * lock holds no work, so it is reclaimed inline the next time someone acquires it. That is why this port
 * has no `sweep`, unlike `QueueStore`.
 */
trait LockStore:

  /**
   * Take `name` if it is free, without waiting; reclaims it when the current lease has expired.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a [[refresh]]
   * @return the hold, carrying the fence token; `None` when it is held under a live lease; aborts with an
   *         `AdapterError` when the store fails
   */
  def tryAcquire(name: String, ttl: Duration): IO[ApplicationError.AdapterError, Option[Hold]]

  /**
   * Take `name`, waiting up to `patience` for a holder to release it.
   *
   * @param name the lock to take
   * @param ttl how long the resulting hold survives without a refresh
   * @param patience the longest to wait
   * @return the hold, or `None` when the wait elapsed first; aborts with an `AdapterError` when the store
   *         fails
   */
  def acquire(name: String, ttl: Duration, patience: Duration): IO[ApplicationError.AdapterError, Option[Hold]]

  /**
   * Release a hold, so a waiter may take it.
   *
   * @param hold the hold returned by an acquire
   * @return true when released, false when the hold had already been revoked; aborts with an `AdapterError`
   *         when the store fails
   */
  def release(hold: Hold): IO[ApplicationError.AdapterError, Boolean]

  /**
   * Push a hold's lease forward.
   *
   * @param hold the hold to keep alive
   * @param ttl how much longer to grant
   * @return the new deadline and whether the hold is still valid; aborts with an `AdapterError` when the
   *         store fails
   */
  def refresh(hold: Hold, ttl: Duration): IO[ApplicationError.AdapterError, (Instant, Boolean)]


object LockStore:

  /**
   * A held lock: the name, the fence token that authorises release and refresh, and the current lease.
   *
   * @param name the lock held
   * @param token the fence generation this hold was granted under
   * @param leaseUntil when the hold lapses unless refreshed, on the store's clock
   */
  final case class Hold(name: String, token: Long, leaseUntil: Instant)
