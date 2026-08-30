package homelab.keyedqueue.e2e


import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration as ProtoDuration
import homelab.keyedqueue.v1.ZioKeyedQueueService.KeyedQueueClient
import homelab.keyedqueue.v1.*
import zio.{ Duration, Task }


/**
 * One deployed dkq, and the four calls a consumer makes against it.
 *
 * Named after its compose service, so a test that kills an instance and a test that talks to one are naming
 * the same thing. The wrapper exists to keep the assertions readable: what a test is *about* is "enqueue on
 * a, dequeue from b", not the proto messages that carry it.
 *
 * @param name the compose service name — also what [[Compose.kill]] takes
 * @param address where it answers, as `host:port`
 * @param client the generated stub, over its own channel
 */
final case class Instance(name: String, address: String, client: KeyedQueueClient):

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
              encoding = Encoding.ENCODING_JSON,
              payload = ByteString.copyFromUtf8(body),
            )
          ),
        )
      )
      .map(_.keyDepth)

  /**
   * Wait for work.
   *
   * @param queue the queue to claim from
   * @param patience how long to block
   * @return the delivery, or nothing when the wait elapsed first; fails when the call does
   */
  def dequeue(queue: String, patience: Duration): Task[Option[Delivery]] =
    client
      .dequeue(DequeueRequest(queue, Some(ProtoDuration(seconds = patience.getSeconds, nanos = patience.getNano))))
      .map(_.delivery)

  /**
   * Report an outcome.
   *
   * @param receipt what the delivery carried
   * @param outcome what the consumer decided
   * @param discardAhead how many messages behind this one to drop as superseded; `OUTCOME_DONE` only
   * @return whether it applied, or was refused as stale; fails when the call does
   */
  def settle(
    receipt: String,
    outcome: Outcome = Outcome.OUTCOME_DONE,
    discardAhead: Int = 0,
  ): Task[Applied] =
    client.settle(SettleRequest(receipt, outcome, discardAhead = discardAhead)).map(_.applied)

  /**
   * Renew everything named.
   *
   * @param receipts what this consumer believes it holds
   * @return the new deadline and whatever it has lost; fails when the call does
   */
  def heartbeat(receipts: Seq[String]): Task[HeartbeatResponse] =
    client.heartbeat(HeartbeatRequest(receipts))
