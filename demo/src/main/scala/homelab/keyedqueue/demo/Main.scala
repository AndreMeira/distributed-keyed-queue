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
 * Talks to `DKQ_ADDRESS`, or `localhost:9000`. Nothing here asserts and nothing here is run by CI — see
 * [[Scenario]] for why that is deliberate.
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
      address <- Client.address
      _       <- Console.printLine(s"→ ${scenario.name}, against $address")
      _       <- Console.printLine(s"  look at: ${scenario.lookAt}")
      _       <- Console.printLine("")
      client  <- Client.scoped(address)
      _       <- scenario.run.provideSomeEnvironment[Scope](_ ++ ZEnvironment(client))
      _       <- Console.printLine(s"← ${scenario.name} done")
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
