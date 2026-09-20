package homelab.keyedqueue.client.queue.managed


import homelab.keyedqueue.client.queue.QueueClient
import homelab.keyedqueue.client.queue.managed.Heartbeat.State
import homelab.keyedqueue.client.queue.model.{ Claim, Receipt, Renewed }
import zio.*


/**
 * The beat that keeps a consumer's claims, and the record of what it is keeping.
 *
 * One call renews every claim at once, so this is one fiber over a registry rather than a fiber per
 * claim. It runs only while something is held: the hold that finds the registry empty starts it, and it
 * stops once the last claim is settled. The cadence is half the lease the service granted, restated by
 * every claim and every renewal, so a ceiling that moves under a running consumer is followed.
 *
 * @param client what the renewals are sent through
 * @param scope what the beat is forked into, whose close ends it
 * @param state what is held and how often to renew it, or nothing on both counts
 */
final private[queue] class Heartbeat(client: QueueClient, scope: Scope, state: Ref[State]):

  /**
   * Take a claim into the set the beat renews, starting the beat if this is the only one.
   *
   * Registering and starting together refuse interruption, so a claim is never left registered with
   * nothing renewing it.
   *
   * @param claim what was granted, whose lease times the beats from here
   * @return noop once it is held
   */
  def hold(claim: Claim): UIO[Unit] =
    ZIO.uninterruptible:
      for
        alone <- register(claim)
        _     <- ZIO.when(alone)(beating.interruptible.forkIn(scope))
      yield ()

  /**
   * Drop a claim from the set the beat renews.
   *
   * The beat stops at its next wake rather than here, so a consumer that claims again within the cadence
   * is served by the fiber already running.
   *
   * @param receipt what was settled, or lost
   * @return noop once it is no longer held
   */
  def release(receipt: Receipt): UIO[Unit] = state.update:
    case State.Idle                    => State.Idle
    case State.Beating(held, interval) => State.Beating(held - receipt, interval)

  /**
   * Renew what is held, for as long as anything is.
   *
   * @return noop once the last claim has been settled
   */
  private def beating: UIO[Unit] = pending.flatMap:
    case None           => ZIO.unit
    case Some(interval) => ZIO.sleep(interval) *> renewing *> beating

  /**
   * Take a claim in, and say whether it is the only one.
   *
   * @param claim what was granted
   * @return whether the registry was empty, so this claim is what starts the beat
   */
  private def register(claim: Claim): UIO[Boolean] = state.modify:
    case State.Idle             => true         -> State.Beating(Set(claim.receipt), claim.leaseTtl.dividedBy(2))
    case State.Beating(held, _) => held.isEmpty -> State.Beating(held + claim.receipt, claim.leaseTtl.dividedBy(2))

  /**
   * How long until the next beat, taking the registry back to idle when it holds nothing.
   *
   * Reading the cadence and standing down are one step, so a claim arriving alongside either starts a
   * beat of its own or is picked up by this one.
   *
   * @return how long to wait, or nothing when there is no longer anything to renew
   */
  private def pending: UIO[Option[Duration]] = state.modify:
    case State.Beating(held, interval) if held.nonEmpty => Some(interval) -> State.Beating(held, interval)
    case _                                              => None           -> State.Idle

  /**
   * Renew what is held, if anything still is.
   *
   * A claim settled while the beat slept leaves nothing to send, and the service allows an empty call but
   * has nothing to do with it. A beat that fails is left to the next one, because the lease outlives a
   * single missed call and there is nobody here to report it to.
   *
   * @return noop once the beat has been sent, or skipped
   */
  private def renewing: UIO[Unit] =
    for
      held <- outstanding
      _    <- ZIO.unless(held.isEmpty)(beat(held))
    yield ()

  /**
   * The receipts held right now.
   *
   * @return what there is to renew, which is none while idle
   */
  private def outstanding: UIO[Set[Receipt]] = state.get.map:
    case State.Idle             => Set.empty
    case State.Beating(held, _) => held

  /**
   * Send one beat for what is held.
   *
   * @param outstanding the receipts to renew
   * @return noop once it has been sent
   */
  private def beat(outstanding: Set[Receipt]): UIO[Unit] =
    client.heartbeat(Chunk.fromIterable(outstanding)).flatMap(renewed).ignore

  /**
   * Take what a beat answered: stop renewing what is no longer held, and keep its cadence.
   *
   * @param answer what the service said
   * @return noop once the registry and the cadence match it
   */
  private def renewed(answer: Renewed): UIO[Unit] = state.update:
    case State.Idle             => State.Idle
    case State.Beating(held, _) => State.Beating(held -- answer.stale, answer.leaseTtl.dividedBy(2))


object Heartbeat:

  /**
   * What a heartbeat is doing: nothing, or keeping claims to a cadence.
   */
  enum State:

    /** Nothing is held, and no fiber is running. */
    case Idle

    /**
     * Claims are held, and a fiber is renewing them.
     *
     * @param held the receipts outstanding, and nothing about what they own
     * @param interval how long to wait before the next beat
     */
    case Beating(held: Set[Receipt], interval: Duration)

  /**
   * A beat that holds nothing and is not yet running.
   *
   * @param client what the renewals are sent through
   * @return the heartbeat, whose fiber lives in the calling scope from the first claim it is given
   */
  def make(client: QueueClient): URIO[Scope, Heartbeat] =
    for
      scope <- ZIO.scope
      state <- Ref.make[State](State.Idle)
    yield Heartbeat(client, scope, state)
