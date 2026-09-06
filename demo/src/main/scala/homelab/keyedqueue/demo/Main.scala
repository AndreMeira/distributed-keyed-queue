package homelab.keyedqueue.demo


import zio.*


/**
 * The demo's entry point: pick a scenario, watch a dashboard.
 *
 * ```
 *   sbt "demo/run"           # the menu
 *   sbt "demo/run steady"    # one scenario
 * ```
 *
 * Talks to `DKQ_ADDRESS`, or `localhost:9000`. That may name several instances, comma-separated, and every
 * scenario spreads its workers over them — which is how a scenario shows whether the service scales out.
 * Nothing here asserts and nothing here is run by CI — see [[Scenario]] for why that is deliberate.
 */
object Main extends ZIOAppDefault:

  /**
   * Run the named scenario, or print the menu.
   *
   * @return noop; fails when the scenario's traffic does
   */
  override def run: ZIO[ZIOAppArgs & Scope, Any, Any] =
    ZIOAppArgs.getArgs.flatMap: args =>
      args.headOption.flatMap(Scenario.byName) match
        case None           => menu
        case Some(scenario) => start(scenario)

  /**
   * Announce what to look at, then produce the traffic.
   *
   * The announcement is the point: a scenario is worth nothing if the person running it does not know
   * which panel it is about.
   *
   * @param scenario what to run
   * @return noop
   */
  private def start(scenario: Scenario): ZIO[Scope, Throwable, Unit] =
    for
      addresses <- Servers.addresses
      _         <- Console.printLine(s"→ ${scenario.name}, against ${addresses.mkString(", ")}")
      _         <- Console.printLine(s"  look at: ${scenario.lookAt}")
      _         <- Console.printLine("")
      servers   <- Servers.scoped(addresses)
      _         <- scenario.run.provideSomeEnvironment[Scope](_ ++ ZEnvironment(servers))
      _         <- Console.printLine(s"← ${scenario.name} done")
    yield ()

  /**
   * What there is to run.
   *
   * @return noop
   */
  private val menu: UIO[Unit] =
    Console
      .printLine(
        s"""usage: sbt "demo/run <scenario>"
           |
           |${Scenario.all.map(s => f"  ${s.name}%-10s ${s.lookAt.linesIterator.next()}").mkString("\n")}
           |
           |Bring the stack up first: bin/run.sh""".stripMargin
      )
      .orDie
