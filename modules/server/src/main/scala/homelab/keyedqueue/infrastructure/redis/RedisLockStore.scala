package homelab.keyedqueue.infrastructure.redis


import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.model.lock.{ Hold, Claim, Position, Turn }
import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.types.{ LockName, Ticket, Token }
import homelab.keyedqueue.infrastructure.redis.script.LockScripts
import homelab.keyedqueue.infrastructure.redis.keys.{ KeyLayout, LockKeys }
import homelab.keyedqueue.infrastructure.redis.script.lock.{ AcquireScript, GrantScript }
import zio.*

import java.time.Instant


/**
 * The lock over Redis: fair by ticket, no sweep.
 *
 * Order lives in the store: a blocking acquire that cannot be granted at once takes a tail ticket in the
 * lock's waiters list, and grants follow ticket order among tickets still within their patience. The
 * instances keep no waiter state, and a newcomer cannot barge past the queue — [[tryAcquire]] refuses while
 * live tickets exist. A dead holder is reclaimed inline by the next grant and a dead waiter is pruned at its
 * own deadline, so nothing sweeps.
 *
 * Every refusal states the delay after which the answer can change, so a waiter has something to park
 * until — the waiting itself is `LockAcquireUseCase`'s. Release and trim append to the wake stream in the
 * same script that frees the lock, so every instance's waiters are woken and only the head ticket can win.
 *
 * See `docs/architecture/lock-mechanics.md` and `docs/architecture/readiness-and-wake.md`.
 *
 * @param monitor what each call on the substrate is traced against
 * @param connection where its connection comes from
 * @param scripts the loaded lock scripts
 */
final class RedisLockStore(
  monitor: Monitor,
  connection: Connection,
  scripts: LockScripts,
  layout: KeyLayout,
) extends LockStore:

  /**
   * One `try` call, which answers at once.
   *
   * Refuses a lock that is free but queued for: a live ticket means someone asked first, and granting past
   * them is the barging the tickets exist to end. The script names the token and the deadline; naming the
   * lock is this adapter's job, since only the caller knows which it asked for.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @return the hold, or `None` when the lock is held or queued for
   */
  override def tryAcquire(name: LockName, ttl: Duration): IO[RedisFailure, Option[Hold]] =
    monitor.trace("RedisLockStore.tryAcquire"):
      connection.provide:
        scripts.tryAcquire.execute(layout.lock(name), name, ttl).map {
          case None          => None
          case Some(granted) => Some(hold(name, granted.token, granted.leaseUntil))
        }

  /**
   * One `acquire` call: the lock, or the ticket that says where the caller stands.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @param within what is left of the caller's patience, which bounds the ticket it may be given
   * @return the hold, or the ticket and the first recheck delay; aborts with `RedisFailure` when the store
   *         fails
   */
  override def place(name: LockName, ttl: Duration, within: Duration): IO[RedisFailure, Position] =
    monitor.trace("RedisLockStore.place"):
      connection.provide:
        scripts.acquire.execute(layout.lock(name), name, ttl, within).map {
          case AcquireScript.Entered.Granted(token, until)   => Position.Granted(hold(name, token, until))
          case AcquireScript.Entered.Queued(ticket, recheck) => Position.Queued(Ticket(ticket), recheck)
        }

  /**
   * One `grant` call: whether it is this ticket's turn yet.
   *
   * @param name the lock queued for
   * @param ttl how long the hold survives without a refresh
   * @param ticket the ticket to ask with
   * @return what the store answered; aborts with `RedisFailure` when the store fails
   */
  override def ask(name: LockName, ttl: Duration, ticket: Ticket): IO[RedisFailure, Turn] =
    // One span per deliberate ask: their count per acquire is the wake-efficiency signal — an event or two
    // each, never a poll's worth.
    monitor.trace("RedisLockStore.ask"):
      connection.provide:
        scripts.grant.execute(layout.lock(name), name, ticket, ttl).map {
          case GrantScript.Asked.Granted(token, until) => Turn.Granted(hold(name, token, until))
          case GrantScript.Asked.Wait(delay)           => Turn.Wait(delay)
          case GrantScript.Asked.Gone                  => Turn.Gone
        }

  /**
   * One `abandon` call, which gives up a place in the queue.
   *
   * @param name the lock queued for
   * @param ticket the ticket to withdraw
   * @return noop; aborts with `RedisFailure` when the store fails
   */
  override def withdraw(name: LockName, ticket: Ticket): IO[RedisFailure, Unit] =
    monitor.trace("RedisLockStore.withdraw"):
      connection.provide:
        scripts.abandon
          .execute(layout.lock(name), name, ticket)
          .unit

  /**
   * One `release` call, which frees the lock and wakes whoever waits on it.
   *
   * @param claim the claim from the hold
   * @return true when released, false when the hold had already been revoked
   */
  override def release(claim: Claim): IO[RedisFailure, Boolean] =
    // The wake is the script's job: lock/release.lua appends to the wake stream on a real release, and the
    // shared listener delivers it to every instance's readiness — so a waiter on another instance wakes,
    // which an in-process call could never reach.
    monitor.trace("RedisLockStore.release"):
      connection.provide:
        scripts.release.execute(layout.lock(claim.name), claim.name, claim.token)

  /**
   * One `refresh` call, which moves the lease if the caller still holds the lock.
   *
   * Token-only, deliberately: a holder whose lease lapsed but whom nobody displaced is late, not lost. The
   * deadline it answers with is meaningless when the hold is gone, which is why the pair says both.
   *
   * @param claim the claim from the hold
   * @param ttl how much longer to grant
   * @return the new deadline, and whether the hold survived to take it
   */
  override def refresh(claim: Claim, ttl: Duration): IO[RedisFailure, (Instant, Boolean)] =
    monitor.trace("RedisLockStore.refresh"):
      connection.provide:
        scripts.refresh
          .execute(layout.lock(claim.name), claim.name, claim.token, ttl)
          .map(_.toTuple)

  /**
   * One `trim` call, which removes holds abandoned past the grace and the waiter lists left with them.
   *
   * Hygiene, not liveness: a lock somebody wants is reclaimed inline by the next grant, so what this frees
   * is what no request would ever reach.
   *
   * @param grace how long past lease expiry a hold survives before it may be removed
   * @param limit the most holds one pass removes
   * @return the names freed, oldest lease first
   */
  override def trim(grace: Duration, limit: Int): IO[RedisFailure, Chunk[LockName]] =
    monitor.trace("RedisLockStore.trim"):
      connection.provide:
        ZIO
          .foreach(layout.locks)(keys => scripts.trim.execute(keys, grace, limit))
          .map(_.flatten)

  /**
   * The hold a granted reply amounts to.
   *
   * The reply carries the token and the deadline; only this caller knows which lock it asked for, which is
   * why the script answers with less than a `Hold`.
   *
   * @param name the lock that was asked for
   * @param token what the grant runs under
   * @param leaseUntil when the lease lapses
   * @return the hold, as the port promises it
   */
  private def hold(name: LockName, token: Token, leaseUntil: Instant): Hold =
    Hold(Claim(name, token), leaseUntil)
