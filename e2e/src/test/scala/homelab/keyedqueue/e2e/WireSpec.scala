package homelab.keyedqueue.e2e


import com.google.protobuf.ByteString
import homelab.keyedqueue.v1.*
import io.grpc.ManagedChannelBuilder
import zio.*
import zio.test.*


/**
 * Does one channel survive concurrent calls?
 *
 * Written to settle a question the rest of the suite could not: the end-to-end tests were failing with HTTP/2
 * corruption — a truncated request seen by the server, then `Incomplete header block fragment` seen by the
 * client — and one bad write desynchronises a connection in *both* directions, so the symptom does not say
 * which side produced it.
 *
 * This drives the same server twice over: once through the Java blocking stub on plain threads, once through
 * the zio-grpc stub on fibers. Both are the canonical way to use their respective client. Whichever corrupts
 * is the guilty one.
 */
object WireSpec extends ZIOSpecDefault:

  private val calls   = 200
  private val threads = 8

  def spec: Spec[TestEnvironment & Scope, Any] = suite("one channel, many callers")(
    test("the Java blocking stub, from plain threads") {
      for
        dkq  <- ZIO.service[Deployment]
        queue = dkq.queue("wire-java")
        // Its own channel, and the Java stub rather than the ZIO one: the point is to be as close to plain
        // gRPC as possible, so that a failure cannot be blamed on anything this repo chose.
        _    <- ZIO.attemptBlocking:
                  val channel = ManagedChannelBuilder.forAddress(dkq.a.host, dkq.a.port).usePlaintext().build()
                  try
                    val stub    = KeyedQueueGrpc.blockingStub(channel)
                    val workers = (0 until threads).map: worker =>
                      val thread = Thread: () =>
                        (0 until calls / threads).foreach: index =>
                          val _ = stub.enqueue(EnqueueRequest(queue, Some(message(s"k$worker", index.toString))))
                      thread.start()
                      thread
                    workers.foreach(_.join())
                  finally
                    val _ = channel.shutdownNow()
      yield assertCompletes
    },
    test("the zio-grpc stub, from fibers") {
      for
        dkq  <- ZIO.service[Deployment]
        queue = dkq.queue("wire-zio")
        _    <- ZIO.foreachParDiscard(0 until threads): worker =>
                  ZIO.foreachDiscard(0 until calls / threads): index =>
                    dkq.a.enqueue(queue, s"k$worker", index.toString)
      yield assertCompletes
    },
  ).provideShared(Deployment.layer) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.timeout(5.minutes)

  /**
   * A message with no interesting content.
   *
   * @param key the key to file it under
   * @param body its payload
   * @return the wire message
   */
  private def message(key: String, body: String): Message =
    Message(
      key = key,
      messageId = s"$key/$body",
      payloadType = "e2e.Text/v1",
      encoding = Encoding.ENCODING_JSON,
      payload = ByteString.copyFromUtf8(body),
    )
