package homelab.keyedqueue.domain.service.lock


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.{ Acquisition, LockClaim }
import homelab.keyedqueue.domain.types.{ LockName, Ticket }
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
   * @param acquisition the lock to take and how long to hold it
   * @param within how much of the caller's patience is left, which bounds the ticket
   * @return the hold, or the ticket and when the answer can next change; aborts with an `AdapterError`
   *         when the store fails
   */
  def place(acquisition: Acquisition, within: Duration): IO[ApplicationError.AdapterError, LockStore.Position]

  /**
   * Ask whether it is this ticket's turn.
   *
   * Does not wait: it answers with the lock, with how long until the answer can change, or with the news
   * that the queue no longer knows this ticket.
   *
   * @param acquisition the lock being queued for and how long to hold it
   * @param ticket the ticket to ask with
   * @return what the store answered; aborts with an `AdapterError` when the store fails
   */
  def ask(acquisition: Acquisition, ticket: Ticket): IO[ApplicationError.AdapterError, LockStore.Turn]

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
   * @param acquisition the lock to take and how long to hold it; its patience is ignored
   * @return the hold, or `None` when it is held under a live lease, or someone queued first; aborts
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

  /**
   * Where a caller stands after asking for a lock: holding it, or queued for it.
   */
  enum Position:

    /**
     * The lock was free and is now this caller's.
     *
     * @param hold what authorises releasing and refreshing it
     */
    case Granted(hold: Hold)

    /**
     * Somebody else has it, and this caller is queued behind them.
     *
     * @param ticket this caller's place
     * @param recheck how long until the answer can change, whatever the wakes say
     */
    case Queued(ticket: Ticket, recheck: Duration)

  /**
   * Whether a ticket's turn has come.
   */
  enum Turn:

    /**
     * The turn came and the lock is this caller's.
     *
     * @param hold what authorises releasing and refreshing it
     */
    case Granted(hold: Hold)

    /**
     * Not yet.
     *
     * @param recheck how long until the answer can change
     */
    case Wait(recheck: Duration)

    /**
     * The queue no longer knows this ticket, so the caller has lost its place and must enter again.
     */
    case Gone
