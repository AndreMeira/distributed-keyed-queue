package homelab.keyedqueue

import zio.*


/**
 * Entry point — a placeholder until there is something to run.
 *
 * The shape to grow into is the one `homelab-toolkit-zio`'s `Main` uses: pattern match on CLI args to pick
 * a run mode (`("server", "inmemory" | "postgres")`, `("database", "migrate")`), so the thing can boot
 * without a database while the design is still moving.
 */
object Main extends ZIOAppDefault:

  override def run: ZIO[Any, Nothing, Unit] =
    Console.printLine("distributed-keyed-queue: nothing wired yet").orDie
