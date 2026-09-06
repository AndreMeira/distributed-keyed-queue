package homelab.keyedqueue.demo


import zio.*


/**
 * One shape of traffic, and what it is worth looking at while it runs.
 *
 * '''A telescope, not a test.''' Nothing here asserts. A scenario exists so a human can watch a panel while
 * a known shape of load goes through the system, which is the one question a test cannot answer: whether
 * the dashboard says anything useful. If a scenario turns up a bug, the bug belongs in `EndToEndSpec` — the
 * demo found it, the suite keeps it found.
 */
trait Scenario:

  /** What to type after `demo/run`. */
  def name: String

  /**
   * Which panel this is about, and what it should do — printed before the traffic starts.
   *
   * The member that makes a scenario usable a month later, when the reason for writing it has been
   * forgotten. A scenario without one is a load generator, which is a less useful thing.
   */
  def lookAt: String

  /**
   * Produce the traffic, and stop on its own.
   *
   * Bounded rather than endless, so a scenario can be watched from start to finish and the next one begins
   * against a quiet system. Scoped because consumers are forked and must die with it — a demo that left
   * fibers running would report its traffic as the next scenario's.
   *
   * Takes [[Servers]] rather than a [[Client]] so that a scenario spreads its workers over however many
   * instances were named, without knowing how many that is.
   *
   * @return noop once the traffic is done; fails when a call to the service does
   */
  def run: ZIO[Servers & Scope, Throwable, Unit]


object Scenario:

  /**
   * Every scenario, in the order they are worth running.
   *
   * @return the catalogue
   */
  val all: Chunk[Scenario] = Chunk(scenario.Steady, scenario.Idle, scenario.Flood)

  /**
   * Find one by name.
   *
   * @param name what the caller typed
   * @return the scenario, or `None` when nothing matches
   */
  def byName(name: String): Option[Scenario] = all.find(_.name == name)
