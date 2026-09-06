package homelab.keyedqueue.demo


import zio.*


/**
 * The instances the demo talks to, and which one a given worker uses.
 *
 * '''Every instance is identical, so spreading load over several is the whole point.''' The service keeps
 * nothing in memory that a second copy would need, so throughput should rise with instances until Redis
 * becomes the limit. A demo that only ever dialled one could not show that, and could not exercise the
 * case the wake path exists for: a consumer parked on one instance, woken by an enqueue that landed on
 * another.
 *
 * '''A worker keeps its instance rather than round-robining per call.''' That is what a real consumer
 * does — it holds a connection — and it keeps a counter off the hot path. Workers are handed out by index
 * modulo the number of instances, so any worker count spreads evenly over any instance count.
 *
 * @param instances the clients, one per address, in the order they were given
 */
final case class Servers(instances: Chunk[Client]):

  /**
   * The instance a worker should use.
   *
   * @param worker the worker's index; any integer, including one past the number of instances
   * @return the client for it
   */
  def apply(worker: Int): Client = instances(math.floorMod(worker, instances.size))

  /** How many instances are being driven. */
  def size: Int = instances.size


object Servers:

  /**
   * Dial every instance, all closed with the scope.
   *
   * @param addresses where they answer, as `host:port`
   * @return the fan-out; fails when a channel cannot be opened
   */
  def scoped(addresses: Chunk[String]): ZIO[Scope, Throwable, Servers] =
    ZIO.foreach(addresses)(Client.scoped).map(Servers.apply)

  /**
   * The instances to talk to, from `DKQ_ADDRESS` or the local default.
   *
   * Comma-separated, so one variable serves both shapes: `localhost:9000` is the single instance
   * `bin/run.sh` starts, and `localhost:9000,localhost:9001` is that one plus a second.
   *
   * @return the addresses, in the order given, never empty
   */
  val addresses: UIO[Chunk[String]] =
    System
      .env("DKQ_ADDRESS")
      .orDie
      .map:
        case None       => Chunk("localhost:9000")
        case Some(text) =>
          val named = Chunk.fromArray(text.split(",")).map(_.trim).filter(_.nonEmpty)
          if named.isEmpty then Chunk("localhost:9000") else named
