package homelab.keyedqueue.domain.service.lock


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.{ Acquisition, LockClaim }
import homelab.keyedqueue.domain.types.LockName
import homelab.keyedqueue.domain.service.lock.LockStore.Hold
import zio.*

import java.time.Instant


/**
 * A distributed lock over a substrate: per-name mutual exclusion, on a renewable lease, with a fence,
 * granted fairly.
 *
 * The sibling of `QueueStore`, and its distilled core — both are a per-key exclusive lease with a fencing
 * token, but a lock carries no messages, no order and no backoff, so it needs far less. See
 * `docs/research/distributed-lock.md`.
 *
 * '''Says nothing about Redis.''' As with the queue, the port is what lets a second substrate exist.
 *
 * '''Reclaim is inline; `trim` is hygiene, not liveness.''' A dead holder whose lock someone wants is
 * reclaimed by their next acquire, so no background pass keeps locks available. What `trim` removes is the
 * holds nobody will ever ask for again, which no request can reach.
 */
trait LockStore:

  /**
   * Take a lock, waiting up to the demand's patience for a holder to release it; reclaims an expired lease
   * inline.
   *
   * '''Fair''': waiters are granted in arrival order, among those still within their patience. A waiter
   * whose patience elapses gives up its place; nothing else reorders the queue.
   *
   * @param acquisition the lock to take, how long to hold it, and how long to wait
   * @return the hold, carrying the claim and lease; `None` when the wait elapsed first; aborts with an
   *         `AdapterError` when the store fails
   */
  def acquire(acquisition: Acquisition): IO[ApplicationError.AdapterError, Option[Hold]]

  /**
   * Take a lock only if it is free now; reclaims an expired lease inline.
   *
   * "Free now" includes free of waiters: when someone queued first, this refuses rather than barge past
   * them.
   *
   * @param acquisition the lock to take and how long to hold it; its patience is ignored
   * @return the hold, or `None` when it is held under a live lease '''or''' someone queued first; aborts
   *         with an `AdapterError` when the store fails
   */
  def tryAcquire(acquisition: Acquisition): IO[ApplicationError.AdapterError, Option[Hold]]

  /**
   * Release a lock this caller holds, so a waiter may take it.
   *
   * @param claim the claim from the hold
   * @return true when released, false when the hold had already been revoked; aborts with an `AdapterError`
   *         when the store fails
   */
  def release(claim: LockClaim): IO[ApplicationError.AdapterError, Boolean]

  /**
   * Push a hold's lease forward.
   *
   * @param claim the claim from the hold
   * @param ttl how much longer to grant
   * @return the new deadline and whether the hold is still valid; aborts with an `AdapterError` when the
   *         store fails
   */
  def refresh(claim: LockClaim, ttl: Duration): IO[ApplicationError.AdapterError, (Instant, Boolean)]

  /**
   * Remove holds whose lease expired longer than `grace` ago, freeing their locks.
   *
   * The grace is part of the refresh contract: a holder may refresh late, up to `grace` past expiry; beyond
   * that its hold may be removed by this pass.
   *
   * @param grace how long past lease expiry a hold survives before it may be removed
   * @param limit the most holds one pass removes
   * @return the names freed, oldest lease first; aborts with an `AdapterError` when the store fails
   */
  def trim(grace: Duration, limit: Int): IO[ApplicationError.AdapterError, Chunk[LockName]]


object LockStore:

  /**
   * A held lock: the claim that authorises releasing or refreshing it, and the current lease.
   *
   * @param claim which lock, under which fence generation — and the handle a caller carries
   * @param leaseUntil when the hold lapses unless refreshed, on the store's clock
   */
  final case class Hold(claim: LockClaim, leaseUntil: Instant)
