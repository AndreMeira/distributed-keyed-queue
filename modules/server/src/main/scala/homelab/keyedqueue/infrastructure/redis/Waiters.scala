package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.QueueName
import zio.*


/**
 * The consumers waiting for work on each queue, and the wake-ups owed to them.
 *
 * A coordination primitive, not a queue: it carries no work and knows nothing about Redis. A wake means
 * "look again", and the caller does the looking — which is what keeps the claim in the fiber that will do
 * the work.
 *
 * '''The shape of a dequeue that uses it.'''
 * {{{
 *   claim()                       // 1. a busy queue never touches this class
 *     .flatMap:
 *       case Some(work) => ZIO.succeed(work)
 *       case None       => waiters.waitFor(queue, patience).flatMap:
 *         case true  => // woken: claim again with what is left of the patience
 *         case false => // patience elapsed: answer empty
 * }}}
 *
 * Claiming *before* waiting is what makes [[Waiters.State.pending]] sound despite being a flag rather than
 * a count: several wake-ups arriving with nobody waiting collapse into one, and the keys they announced are
 * found by the next caller's claim, which always precedes its wait.
 *
 * @param state the waiters and the flags, in one ref because a registration and a delivery both span both
 */
final class Waiters(state: Ref.Synchronized[Waiters.State]):

  /**
   * Wait for a wake on `queue`, for at most `patience`.
   *
   * Answers `true` when it was woken — including immediately, when a wake had arrived with nobody to take
   * it — and `false` when the patience elapsed. An interrupted caller answers nothing, but still hands on
   * anything it was given.
   *
   * @param queue what to wait for work on
   * @param patience the longest to wait
   * @return whether a wake arrived
   */
  def waitFor(queue: QueueName, patience: Duration): UIO[Boolean] =
    // Uninterruptible around the registration, interruptible only inside the wait. Without the mask there
    // is a window between joining the queue and attaching the exit handler: a caller interrupted there
    // leaves a live promise behind that nothing will ever settle, and the next wake is handed to a fiber
    // that no longer exists — lost, with a consumer asleep beside claimable work.
    ZIO.uninterruptibleMask: restore =>
      ticket(queue).flatMap:
        case Waiters.Ticket.Ready         => ZIO.succeed(true)
        case Waiters.Ticket.Wait(promise) =>
          restore(promise.await.timeout(patience).map(_.isDefined))
            .onExit(exit => settle(queue, promise, exit))

  /**
   * Hand a wake to one waiter on `queue`, or remember it for the next one.
   *
   * '''One wake, one waiter.''' An entry means one key became claimable, so waking every waiter would give
   * one claim and the rest a wasted look. Waiters are taken oldest first; ones that have given up are
   * skipped and dropped on the way past, which is why a caller that times out never has to remove itself
   * under contention.
   *
   * @param queue what became claimable
   * @return noop
   */
  def wake(queue: QueueName): UIO[Unit] =
    state.updateZIO(handOn(_, queue))

  /**
   * How many callers are waiting on a queue, live or given up.
   *
   * For tests and metrics; a number that includes waiters whose patience has expired but which no delivery
   * has walked past yet.
   *
   * @param queue the queue to count
   * @return the number of registrations held
   */
  def waiting(queue: QueueName): UIO[Int] =
    state.get.map(_.waiting.getOrElse(queue, Chunk.empty).size)

  /**
   * Take a wake that was owed, or join the queue for the next one.
   *
   * '''One atomic decision.''' Checking the flag and registering cannot be two steps: a delivery landing
   * between them would complete a registration that is about to be abandoned. Answering with either
   * `Ready` or `Wait` — never both, never a half state — is what removes that window, and it is why the
   * flags and the waiters live in one ref.
   *
   * @param queue the queue being waited on
   * @return `Ready` when a wake was owed, or a registration to await
   */
  private def ticket(queue: QueueName): UIO[Waiters.Ticket] =
    state.modifyZIO: current =>
      if current.pending.contains(queue)
      then ZIO.succeed(Waiters.Ticket.Ready -> current.copy(pending = current.pending - queue))
      else
        Promise
          .make[Nothing, Unit]
          .map: promise =>
            val queued = current.waiting.getOrElse(queue, Chunk.empty) :+ promise
            Waiters.Ticket.Wait(promise) -> current.copy(waiting = current.waiting.updated(queue, queued))

  /**
   * Leave the queue, handing on a wake that arrived too late to be used.
   *
   * '''The promise is the arbiter.''' `succeed` and `interrupt` both answer whether *this* call completed
   * it, so exactly one of "the caller gave up" and "a wake was delivered" wins. A caller that lost that
   * race is holding a wake it can no longer use, and dropping it would leave a key claimable with nobody
   * looking — so it delivers it onwards, exactly as the listener would.
   *
   * '''One transition, not three.''' Removing the registration, deciding the race and handing the wake on
   * all happen inside a single `modifyZIO`, because they are one change to one piece of state. Done as
   * separate updates, a concurrent [[wake]] could set `pending` between the removal and the hand-on and
   * have it overwritten — a lost wake that appears once in a few hundred interleavings.
   *
   * Runs on every exit, including interruption, which is why it is attached with `onExit` rather than
   * written after the await.
   *
   * @param queue the queue waited on
   * @param promise this caller's registration
   * @param exit how the wait ended
   * @return noop
   */
  private def settle(queue: QueueName, promise: Promise[Nothing, Unit], exit: Exit[Nothing, Boolean]): UIO[Unit] =
    state.updateZIO: current =>
      val without = forget(current, queue, promise)
      exit match
        case Exit.Success(true) => ZIO.succeed(without) // woken, and the wake was used
        case _                  =>
          promise.interrupt.flatMap:
            case true  => ZIO.succeed(without) // we ended the wait first; nothing was delivered to us
            case false => handOn(without, queue)

  /**
   * Drop a registration from a state.
   *
   * Deliveries skip completed promises anyway, so this is tidying rather than correctness — it keeps a
   * queue that is waited on often from accumulating spent registrations between wakes.
   *
   * @param current the state to remove from
   * @param queue the queue waited on
   * @param promise the registration to drop
   * @return the state without it
   */
  private def forget(current: Waiters.State, queue: QueueName, promise: Promise[Nothing, Unit]): Waiters.State =
    current.waiting.get(queue) match
      case None       => current
      case Some(held) =>
        val rest = held.filterNot(_ == promise)
        if rest.isEmpty then current.copy(waiting = current.waiting - queue)
        else current.copy(waiting = current.waiting.updated(queue, rest))

  /**
   * Give a wake nobody used to the next waiter, or remember it.
   *
   * The same operation [[wake]] performs, reached from the other side: a caller that lost the race for its
   * own promise is holding a wake, and holding is the one thing it must not do.
   *
   * @param current the state to deliver within
   * @param queue the queue the wake was for
   * @return the state after the hand-on
   */
  private def handOn(current: Waiters.State, queue: QueueName): UIO[Waiters.State] =
    deliver(current.waiting.getOrElse(queue, Chunk.empty)).map:
      case Some(rest) => current.copy(waiting = current.waiting.updated(queue, rest))
      case None       => Waiters.State(current.waiting - queue, current.pending + queue)

  /**
   * Give the wake to the first waiter still willing to take it.
   *
   * @param waiting the registrations for one queue, oldest first
   * @return what is left of them once one has been woken, or `None` when none were live
   */
  private def deliver(waiting: Chunk[Promise[Nothing, Unit]]): UIO[Option[Chunk[Promise[Nothing, Unit]]]] =
    waiting.headOption match
      case None          => ZIO.none
      case Some(promise) =>
        promise
          .succeed(())
          .flatMap:
            case true  => ZIO.some(waiting.drop(1))
            case false => deliver(waiting.drop(1))


object Waiters:

  /**
   * Who is waiting, and what is owed.
   *
   * @param waiting queue → its registrations, oldest first
   * @param pending queues a wake arrived for with nobody live to take it. A set rather than a count: the
   *                next caller claims before it waits, so it finds whatever the collapsed wakes announced
   */
  final case class State(waiting: Map[QueueName, Chunk[Promise[Nothing, Unit]]], pending: Set[QueueName])

  /** The answer to "am I owed a wake, or do I have to wait for one". */
  private enum Ticket:

    /** A wake was owed and has been taken; look again now. */
    case Ready

    /** Registered; await this. */
    case Wait(promise: Promise[Nothing, Unit])

  /**
   * An empty registry.
   *
   * @return the waiters
   */
  def make: UIO[Waiters] = Ref.Synchronized.make(State(Map.empty, Set.empty)).map(Waiters(_))
