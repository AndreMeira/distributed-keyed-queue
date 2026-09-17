package homelab.keyedqueue.domain.service.persistence


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.lock.{ Hold, LockClaim, Position, Turn }
import homelab.keyedqueue.domain.types.{ LockName, Ticket }
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
 * Reclaim is inline: a dead holder whose lock someone wants is reclaimed by the next acquire, so no
 * background pass keeps locks available. What `trim` removes is the holds nobody will ever ask for again,
 * which no request can reach — hygiene rather than liveness.
 */
trait LockStore:

  /**
   * Ask for the lock, and take a place in its queue when it cannot be granted at once.
   *
   * Does not wait: it answers with the lock or with the ticket that says where the caller stands. The
   * ticket carries a deadline of `within` from now, so a caller that gives up early stops delaying the
   * waiters behind it.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @param within how much of the caller's patience is left, which bounds the ticket
   * @return the hold, or the ticket and when the answer can next change; aborts with an `AdapterError`
   *         when the store fails
   */
  def place(name: LockName, ttl: Duration, within: Duration): IO[ApplicationError.AdapterError, Position]

  /**
   * Ask whether it is this ticket's turn.
   *
   * Does not wait: it answers with the lock, with how long until the answer can change, or with the news
   * that the queue no longer knows this ticket.
   *
   * @param name the lock being queued for
   * @param ttl how long the hold survives without a refresh
   * @param ticket the ticket to ask with
   * @return what the store answered; aborts with an `AdapterError` when the store fails
   */
  def ask(name: LockName, ttl: Duration, ticket: Ticket): IO[ApplicationError.AdapterError, Turn]

  /**
   * Give up a place in the queue.
   *
   * Best effort: a withdrawal that does not land leaves a ticket the store prunes at its own deadline.
   *
   * @param name the lock queued for
   * @param ticket the ticket to withdraw
   * @return noop; aborts with an `AdapterError` when the store fails
   */
  def withdraw(name: LockName, ticket: Ticket): IO[ApplicationError.AdapterError, Unit]

  /**
   * Take a lock only if it is free now; reclaims an expired lease inline.
   *
   * "Free now" includes free of waiters: when someone queued first, this refuses rather than barge past
   * them.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @return the hold, or `None` when it is held under a live lease, or someone queued first; aborts
   *         with an `AdapterError` when the store fails
   */
  def tryAcquire(name: LockName, ttl: Duration): IO[ApplicationError.AdapterError, Option[Hold]]

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
