package homelab.keyedqueue.client.queue

import zio.*


/**
 * The beat that keeps a consumer's claims, and the record of what it is keeping.
 *
 * One call renews every claim at once, so this is one fiber over a registry rather than a fiber per
 * claim. The cadence comes from the lease the service granted, and every renewal restates it, so a
 * ceiling that moves under a running consumer is followed.
 *
 * @param client what the renewals are sent through
 * @param held the receipts outstanding, and nothing about what they own
 * @param cadence how long to wait before the next beat
 */
final private[queue] class Heartbeat(client: QueueClient, held: Ref[Set[Receipt]], cadence: Ref[Duration]):

  /**
   * Take a claim into the set the beat renews.
   *
   * @param claim what was granted
   * @return noop once it is held
   */
  def hold(claim: Claim): UIO[Unit] =
    held.update(_ + claim.receipt) *> cadence.set(claim.leaseTtl.dividedBy(2))

  /**
   * Drop a claim from the set the beat renews.
   *
   * @param receipt what was settled, or lost
   * @return noop once it is no longer held
   */
  def release(receipt: Receipt): UIO[Unit] =
    held.update(_ - receipt)

  /**
   * Renew everything held, for as long as this runs.
   *
   * Never completes, so it is forked: whoever starts it decides how long it lives, and the claims it was
   * keeping lapse on their own leases once it stops.
   *
   * @return never completes
   */
  def start: UIO[Nothing] =
    (waiting *> renewing).forever

  /**
   * Wait out the current cadence.
   *
   * @return noop when the next beat is due
   */
  private def waiting: UIO[Unit] =
    for
      interval <- cadence.get
      _        <- ZIO.sleep(interval)
    yield ()

  /**
   * Renew what is held, if anything is.
   *
   * An empty registry is not a call: the service allows one, but a consumer between claims has nothing
   * for it to renew. A beat that fails is left to the next one, because the lease outlives a single
   * missed call and there is nobody here to report it to.
   *
   * @return noop once the beat has been sent, or skipped
   */
  private def renewing: UIO[Unit] =
    for
      outstanding <- held.get
      _           <- ZIO.unless(outstanding.isEmpty)(beat(outstanding))
    yield ()

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
  private def renewed(answer: Renewed): UIO[Unit] =
    held.update(_ -- answer.stale) *> cadence.set(answer.leaseTtl.dividedBy(2))


object Heartbeat:

  /**
   * A beat that holds nothing yet, keeping to the given cadence until a claim states its own.
   *
   * @param client what the renewals are sent through
   * @param idle how long to wait between beats before anything has been claimed
   * @return the heartbeat
   */
  def make(client: QueueClient, idle: Duration): UIO[Heartbeat] =
    for
      held    <- Ref.make(Set.empty[Receipt])
      cadence <- Ref.make(idle)
    yield Heartbeat(client, held, cadence)
