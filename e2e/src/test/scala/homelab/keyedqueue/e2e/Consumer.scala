package homelab.keyedqueue.e2e


import zio.*

import java.time.Instant


/**
 * The loop a real consumer writes: dequeue, work, settle, repeat — with a record of what it saw.
 *
 * The record is what the assertions are made of, so how it is taken matters as much as what it contains.
 * Both decisions below exist to make a *failure* mean something:
 *
 *   1. '''The record is written before the settle is sent.''' A key becomes claimable again only once its
 *      settle has applied, so recording first puts this handling in the log strictly before the next
 *      consumer can claim the same key. Record afterwards and two consumers race to append, and a
 *      per-key order assertion fails for a reason that has nothing to do with the queue.
 *   2. '''The held window ends before the settle, not after it.''' `claimedAt` is taken once the claim has
 *      been granted and `releasedAt` before the settle leaves — an interval strictly inside the one the
 *      server considers the key held. An overlap between two windows is therefore a real overlap, never a
 *      round trip masquerading as one.
 */
object Consumer:

  /**
   * One message, worked to completion.
   *
   * @param instance which instance served it
   * @param key the key it was ordered by
   * @param body its payload as text
   * @param attempt what the delivery claimed — 1 unless it had been reclaimed
   * @param claimedAt when this consumer took it
   * @param releasedAt when it stopped holding it
   */
  final case class Handled(
    instance: String,
    key: String,
    body: String,
    attempt: Int,
    claimedAt: Instant,
    releasedAt: Instant,
  )

  /**
   * Consume until the queue stops offering work.
   *
   * Stops on the first empty reply, which makes `patience` the definition of "drained": long enough that a
   * consumer waiting on a key another consumer is holding does not give up early.
   *
   * @param instance the instance to consume from
   * @param queue the queue to drain
   * @param patience how long each dequeue blocks before concluding the queue is empty
   * @param hold how long to pretend to work; zero for throughput, non-zero to make holds overlap if they can
   * @param into where handled messages accumulate, shared across every consumer in a test
   * @return how many this consumer handled; fails when a call does
   */
  def drain(instance: Instance, queue: String, patience: Duration, hold: Duration, into: Ref[Chunk[Handled]]): Task[Int] =
    step(instance, queue, patience, hold, into, 0)

  /**
   * One message, then the rest.
   *
   * @param instance the instance to consume from
   * @param queue the queue to drain
   * @param patience how long this dequeue blocks
   * @param hold how long to hold the message before settling
   * @param into where to record it
   * @param handled how many this consumer has taken so far
   * @return the running total once the queue runs dry; fails when a call does
   */
  private def step(
    instance: Instance,
    queue: String,
    patience: Duration,
    hold: Duration,
    into: Ref[Chunk[Handled]],
    handled: Int,
  ): Task[Int] =
    instance
      .dequeue(queue, patience)
      .flatMap: reply =>
        // One message at a time: this consumer exists to exercise per-key exclusivity, and a batch would
        // put several of one key's messages in its hands at once — a different property, tested elsewhere.
        reply.head match
          case None           => ZIO.succeed(handled)
          case Some(delivery) =>
            for
              claimedAt  <- Clock.instant
              _          <- ZIO.sleep(hold).when(hold > Duration.Zero)
              releasedAt <- Clock.instant
              message     = delivery.message
              _          <- into.update(
                              _ :+ Handled(
                                instance = instance.name,
                                key = message.map(_.key).getOrElse(""),
                                body = message.map(_.payload.toStringUtf8).getOrElse(""),
                                attempt = delivery.attempt,
                                claimedAt = claimedAt,
                                releasedAt = releasedAt,
                              )
                            )
              _          <- instance.settle(reply)
              total      <- step(instance, queue, patience, hold, into, handled + 1)
            yield total
