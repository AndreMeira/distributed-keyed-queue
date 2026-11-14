package homelab.keyedqueue.infrastructure.redis


import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.model.queue.{ Claim, Grant, ScheduledSubmission, Settlement, Submission }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.keys.{ BackoffKeys, KeyLayout, QueueKeys, ScheduleKeys }
import homelab.keyedqueue.infrastructure.redis.script.QueueScripts
import homelab.keyedqueue.infrastructure.redis.script.queue.{ BackoffScript, ClaimScript, MoveScript, RenewScript }
import zio.*

import java.time.Instant


/**
 * The queue over Redis.
 *
 * Nothing here blocks in Redis: every operation is one script that answers at once, so they all share a
 * single connection. The only command in the process that parks is the wake path's `XREAD`, on connections
 * of its own — see [[WakeConsumer]].
 *
 * A claim is granted in one call, so a key is either queued or claimed and the lease is the only thing that
 * expires: no connection announces itself, and nothing has to be kept alive on its behalf.
 *
 * Everything that reaches the substrate opens a span, and none of it records a metric — the RPC above
 * already counts and times what a caller asked for. `attempt` is traced although it is private, because it
 * is the one that repeats: its span count shows the redundant attempts a lost race pays for.
 *
 * See `docs/architecture/redis-connections.md` and `docs/research/redis-keyed-queue.md`.
 *
 * @param monitor what each call on the substrate is traced against
 * @param connection where its connection comes from
 * @param scripts the loaded script digests
 * @param leaseTtl how long a claim survives without a heartbeat
 */
final class RedisQueueStore(
  monitor: Monitor,
  connection: Connection,
  scripts: QueueScripts,
  layout: KeyLayout,
  val leaseTtl: Duration,
) extends QueueStore:

  /**
   * One `enqueue` call. The conditional add inside it is what keeps a key in `ready` at most once, and so
   * cannot be split into a read and a write here.
   *
   * The message is serialised here rather than by the caller: what a message looks like at rest is this
   * adapter's choice — see [[StoredMessage]].
   *
   * @param submission the queue to append in, and the message; the key it carries decides where it lands
   * @return the key's depth after the append
   */
  override def enqueue(submission: Submission): IO[RedisFailure, Long] =
    monitor.trace("RedisQueueStore.enqueue"):
      connection.provide:
        scripts.enqueue.execute(layout.queue(submission.queue), submission.message)

  /**
   * Claim whatever is claimable, without waiting.
   *
   * @param queue the queue to claim from
   * @param batch how much to take
   * @return the claim, or `None` when nothing was claimable; aborts with `RedisFailure` when the store fails
   */
  override def attemptClaim(queue: QueueName, batch: Int): IO[RedisFailure, Option[Grant]] =
    monitor.trace("RedisQueueStore.attemptClaim"):
      connection.provide:
        val keys = layout.queue(queue)
        scripts.claim
          .execute(keys, leaseTtl, batch)
          .map:
            case None          => None
            case Some(claimed) => Some(granted(keys, claimed))

  /**
   * One `settle` call, which checks the token every time and advances it only when the claim ends — a
   * partial settle has to leave the receipt usable for the rest of the batch.
   *
   * So it is not the fence that makes a replay harmless here: settling removes the id from the claim's
   * owned set, and removing it a second time finds nothing. The fence is what invalidates the receipt
   * once the claim is over, or once the watchdog has revoked it.
   *
   * @param settlement the claim, what became of the messages it names, and any backoff
   * @return whether it applied
   */
  override def settle(settlement: Settlement): IO[RedisFailure, Boolean] =
    monitor.trace("RedisQueueStore.settle"):
      connection.provide:
        scripts.settle.execute(layout.queue(settlement.claimed.queue), settlement)

  /**
   * One `renew` call per queue, because claims are namespaced by queue while a caller's receipts are not:
   * a consumer holding work in three queues costs three round trips here.
   *
   * No worker entry is written, because there are none: a consumer is known by its receipts, and its
   * claims are found by fence token.
   *
   * @param claims every claim the caller still holds, across any number of queues
   * @return the new deadline and the claims already revoked
   */
  override def renew(claims: Chunk[Claim]): IO[RedisFailure, (Instant, Chunk[Claim])] =
    monitor.trace("RedisQueueStore.renew"):
      connection.provide:
        ZIO
          .foreach(claims.groupBy(_.queue).toList): (queue, held) =>
            scripts.renew.execute(layout.queue(queue), leaseTtl, held).map(renewed(held, _))
          .map: results =>
            val (when, chunk) = results.unzip
            when.maxOption.getOrElse(Instant.EPOCH) -> Chunk.fromIterable(chunk).flatten

  /**
   * One `sweep` call, the reclaim of lapsed claims.
   *
   * @param queue the queue to repair
   * @param limit the most entries to handle in one pass
   * @return what it repaired
   */
  override def sweep(queue: QueueName, limit: Int): IO[RedisFailure, QueueStore.Swept] =
    monitor.trace("RedisQueueStore.sweep"):
      connection.provide:
        scripts.sweep.execute(layout.queue(queue), limit)

  /**
   * One `schedule` call, in the partition the queue falls in. Whether the id is already scheduled and the
   * recording are one script, so two racing schedules for one id cannot both be recorded.
   *
   * @param scheduled where the message goes, the message, and how long until it is due
   * @return whether it was recorded, and how long until the recorded schedule is due
   */
  override def schedule(scheduled: ScheduledSubmission): IO[RedisFailure, QueueStore.Scheduled] =
    monitor.trace("RedisQueueStore.schedule"):
      connection.provide:
        scripts.schedule.execute(layout.schedule(scheduled.submission.queue), scheduled)

  /**
   * One `move` call per partition, all of them at once and every one tried even when another fails, so an
   * unreachable node holds back only what it holds rather than every partition after it.
   *
   * Each partition's next due time is "in X ms" as of its own reply; by the time the last reply arrives the
   * earlier ones have aged, so each is shortened by the time elapsed since it was read.
   *
   * @param limit the most entries one partition's call moves
   * @return the partitions' results combined, with how many could not be reached; aborts with the first
   *         partition's failure only when none could be
   */
  override def moveDue(limit: Int): IO[RedisFailure, QueueStore.Moved] =
    monitor.trace("RedisQueueStore.moveDue"):
      connection.provide:
        for
          results <- ZIO.partitionPar(layout.schedules)(moveIn(limit)).withParallelism(layout.partitions)
          now     <- Clock.instant
          moved   <- combined(results, now)
        yield moved

  /**
   * One partition's move, with the local time its reply arrived.
   *
   * @param limit the most entries to move
   * @param keys the partition's schedule keys
   * @return what the partition's call did, and when its answer was read; aborts with `RedisFailure` when it fails
   */
  private def moveIn(limit: Int)(keys: ScheduleKeys): ZIO[Connection.Commands, RedisFailure, (MoveScript.Output, Instant)] =
    scripts.move.execute(keys, limit).zip(Clock.instant)

  /**
   * The partitions' results as one, or the first failure when no partition answered.
   *
   * @param results the failures, then the partitions' answers with the time each was read
   * @param now the time the last of them was in, which each answer's next due time is aged to
   * @return the combined result; aborts with the first failure when there is no answer at all
   */
  private def combined(
    results: (Iterable[RedisFailure], Iterable[(MoveScript.Output, Instant)]),
    now: Instant,
  ): IO[RedisFailure, QueueStore.Moved] =
    val (failures, answers) = results
    failures.headOption match
      case Some(failure) if answers.isEmpty => ZIO.fail(failure)
      case _                                =>
        ZIO.succeed(answers.foldLeft(QueueStore.Moved(0, 0, false, None, failures.size))(add(now)))

  /**
   * One partition's answer added to the total, its next due time aged to `now`.
   *
   * @param now the time the total is as of
   * @param total what the partitions so far did
   * @param answer one more partition's answer, and when it was read
   * @return the total with it: counts summed, full if any was, and the earliest next due time
   */
  private def add(now: Instant)(total: QueueStore.Moved, answer: (MoveScript.Output, Instant)): QueueStore.Moved =
    val (output, read) = answer
    QueueStore.Moved(
      moved = total.moved + output.moved,
      discarded = total.discarded + output.discarded,
      full = total.full || output.full,
      nextDueIn = earliest(total.nextDueIn, aged(output.nextDueIn, Duration.fromInterval(read, now))),
      failed = total.failed,
    )

  /**
   * One `backoff` call per partition, all of them at once and every one tried even when another fails, as for
   * [[moveDue]]; each partition's next end is aged to when the last reply arrived.
   *
   * @param limit the most backoffs one partition's call releases
   * @return the partitions' results combined, with how many could not be reached; aborts with the first
   *         partition's failure only when none could be
   */
  override def releaseDue(limit: Int): IO[RedisFailure, QueueStore.Released] =
    monitor.trace("RedisQueueStore.releaseDue"):
      connection.provide:
        for
          results  <- ZIO.partitionPar(layout.backoffs)(releaseIn(limit)).withParallelism(layout.partitions)
          now      <- Clock.instant
          released <- releasedAll(results, now)
        yield released

  /**
   * One partition's release, with the local time its reply arrived.
   *
   * @param limit the most backoffs to release
   * @param keys the partition's backoff keys
   * @return what the partition's call did, and when its answer was read; aborts with `RedisFailure` when it fails
   */
  private def releaseIn(limit: Int)(keys: BackoffKeys): ZIO[Connection.Commands, RedisFailure, (BackoffScript.Output, Instant)] =
    scripts.backoff.execute(keys, limit).zip(Clock.instant)

  /**
   * The partitions' releases as one, or the first failure when no partition answered.
   *
   * @param results the failures, then the partitions' answers with the time each was read
   * @param now the time the last of them was in, which each answer's next end is aged to
   * @return the combined result; aborts with the first failure when there is no answer at all
   */
  private def releasedAll(
    results: (Iterable[RedisFailure], Iterable[(BackoffScript.Output, Instant)]),
    now: Instant,
  ): IO[RedisFailure, QueueStore.Released] =
    val (failures, answers) = results
    failures.headOption match
      case Some(failure) if answers.isEmpty => ZIO.fail(failure)
      case _                                =>
        ZIO.succeed(answers.foldLeft(QueueStore.Released(0, false, None, failures.size))(addReleased(now)))

  /**
   * One partition's release added to the total, its next end aged to `now`.
   *
   * @param now the time the total is as of
   * @param total what the partitions so far did
   * @param answer one more partition's answer, and when it was read
   * @return the total with it: counts summed, full if any was, and the earliest next end
   */
  private def addReleased(now: Instant)(total: QueueStore.Released, answer: (BackoffScript.Output, Instant)): QueueStore.Released =
    val (output, read) = answer
    QueueStore.Released(
      released = total.released + output.released,
      full = total.full || output.full,
      nextDueIn = earliest(total.nextDueIn, aged(output.nextDueIn, Duration.fromInterval(read, now))),
      failed = total.failed,
    )

  /**
   * A next due time shortened by the time that has passed since it was read.
   *
   * @param nextDueIn the delay as read, if any
   * @param elapsed how long ago it was read
   * @return the delay that remains, never negative; `None` when there was none
   */
  private def aged(nextDueIn: Option[Duration], elapsed: Duration): Option[Duration] =
    nextDueIn match
      case Some(delay) => Some((delay minus elapsed).max(Duration.Zero))
      case None        => None

  /**
   * The earlier of two optional delays.
   *
   * @param left one
   * @param right the other
   * @return the earlier, or whichever is present, or `None`
   */
  private def earliest(left: Option[Duration], right: Option[Duration]): Option[Duration] =
    (left, right) match
      case (Some(a), Some(b)) => Some(a.min(b))
      case (Some(a), None)    => Some(a)
      case (None, other)      => other

  /**
   * The claim a granted reply amounts to.
   *
   * The reply names the key the script chose; only this caller knows which queue it was chosen from, which
   * is why the script answers with less than a `Grant`.
   *
   * @param keys the queue that was claimed from
   * @param claimed what the script granted
   * @return the claim, as the port promises it
   */
  private def granted(keys: QueueKeys, claimed: ClaimScript.Claimed): Grant =
    Grant(Claim(keys.queue, claimed.key, claimed.token), claimed.batch, claimed.deadline, leaseTtl, claimed.backlog)

  /**
   * Match the keys a beat could not renew back to the claims it was sent with.
   *
   * The script reports keys, and the caller has to stop working *claims* — a key alone would not say which
   * claim it lost, which is why only the beat that was sent can pair them up.
   *
   * @param held the claims the beat carried
   * @param reply the deadline every renewed claim now carries, and the keys that were not renewed
   * @return the deadline, and the claims that had already been revoked
   */
  private def renewed(held: Chunk[Claim], reply: RenewScript.Output): (Instant, Chunk[Claim]) =
    (reply.renewedUntil, held.filter(claim => reply.lost.toSet.contains(claim.key)))
