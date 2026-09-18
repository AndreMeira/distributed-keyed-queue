package homelab.keyedqueue.e2e


import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration as ProtoDuration
import homelab.keyedqueue.v1.ZioKeyedLockService.KeyedLockClient
import homelab.keyedqueue.v1.ZioKeyedQueueService.KeyedQueueClient
import homelab.keyedqueue.v1.*
import zio.{ Duration, Task }


/**
 * One deployed dkq, and the calls a consumer makes against it — the queue's four and the lock's three.
 *
 * Named after its compose service, so a test that kills an instance and a test that talks to one are naming
 * the same thing. The wrapper exists to keep the assertions readable: what a test is *about* is "enqueue on
 * a, dequeue from b", not the proto messages that carry it.
 *
 * @param name the compose service name — also what [[Compose.kill]] takes
 * @param address where it answers, as `host:port`
 * @param client the generated queue stub, over its own channel
 * @param locks the generated lock stub, over its own channel
 */
final case class Instance(name: String, address: String, client: KeyedQueueClient, locks: KeyedLockClient):

  /** The host half of [[address]]. */
  def host: String = address.split(":").head

  /** The port half of [[address]]. */
  def port: Int = address.split(":").last.toInt

  /**
   * Send a message.
   *
   * @param queue the queue to send to
   * @param key the key that orders it
   * @param body the payload, as text — these tests care about identity and order, not about content
   * @return the key's depth after the append; fails when the call does
   */
  def enqueue(queue: String, key: String, body: String): Task[Long] =
    client
      .enqueue(
        EnqueueRequest(
          queue,
          Some(
            Message(
              key = key,
              messageId = s"$key/$body",
              payloadType = "e2e.Text/v1",
              encoding = "application/json",
              payload = ByteString.copyFromUtf8(body),
            )
          ),
        )
      )
      .map(_.keyDepth)

  /**
   * Claim a batch of one key's messages.
   *
   * @param queue the queue to take from
   * @param patience how long to block
   * @param maxBatch the most messages to claim at once
   * @return the response, empty when the wait elapsed first; fails when the call does
   */
  def dequeue(queue: String, patience: Duration, maxBatch: Int = 1): Task[DequeueResponse] =
    client.dequeue(
      DequeueRequest(
        queue,
        Some(ProtoDuration(seconds = patience.getSeconds, nanos = patience.getNano)),
        maxBatch = maxBatch,
      )
    )

  /**
   * Report what became of some of a claim's messages.
   *
   * @param receipt the claim, as the response carried it
   * @param outcomes what became of each message named, by id
   * @return whether it applied, or was refused as stale; fails when the call does
   */
  def settleEach(receipt: String, outcomes: Seq[MessageOutcome]): Task[Applied] =
    client.settle(SettleRequest(receipt, outcomes = outcomes)).map(_.applied)

  /**
   * Report the same outcome for everything a claim handed over.
   *
   * @param reply what the dequeue returned
   * @param outcome what became of every message in it
   * @return whether it applied, or was refused as stale; fails when the call does
   */
  def settle(reply: DequeueResponse, outcome: Outcome = Outcome.OUTCOME_DONE): Task[Applied] =
    settleEach(reply.receipt, Instance.claimed(reply).map(one => MessageOutcome(one.messageId, outcome)))

  /**
   * Renew everything named.
   *
   * @param receipts what this consumer believes it holds
   * @return the new deadline and whatever it has lost; fails when the call does
   */
  def heartbeat(receipts: Seq[String]): Task[HeartbeatResponse] =
    client.heartbeat(HeartbeatRequest(receipts))

  /**
   * Take a named lock, waiting up to `patience`.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @param patience how long to wait for a holder to let go
   * @return the response — `acquired = false` when the wait elapsed; fails when the call does
   */
  def acquire(name: String, ttl: Duration, patience: Duration): Task[AcquireResponse] =
    locks.acquire(AcquireRequest(name, Some(Instance.wire(ttl)), Some(Instance.wire(patience))))

  /**
   * Free a held lock.
   *
   * @param receipt the handle the acquire returned
   * @return whether it applied — false when the hold had already been revoked; fails when the call does
   */
  def release(receipt: String): Task[Boolean] =
    locks.release(ReleaseRequest(receipt)).map(_.released)

  /**
   * Push a held lock's lease forward.
   *
   * @param receipt the handle the acquire returned
   * @param ttl how much longer to grant
   * @return the response — `renewed = false` when the hold has been lost; fails when the call does
   */
  def refresh(receipt: String, ttl: Duration): Task[RefreshResponse] =
    locks.refresh(RefreshRequest(receipt, Some(Instance.wire(ttl))))


object Instance:

  /**
   * Everything a claim handed over, head first.
   *
   * The response keeps the first message apart from the rest, so that "did I get anything" is one question
   * and not an emptiness check. A test that wants to iterate wants them back together.
   *
   * @param reply what the dequeue returned
   * @return the messages, in producer order; empty when nothing was claimed
   */
  def claimed(reply: DequeueResponse): Seq[Delivery] = reply.head.toSeq ++ reply.tail

  /**
   * A duration as the wire carries it.
   *
   * @param duration the duration
   * @return its proto form
   */
  def wire(duration: Duration): ProtoDuration = ProtoDuration(duration.getSeconds, duration.getNano)
