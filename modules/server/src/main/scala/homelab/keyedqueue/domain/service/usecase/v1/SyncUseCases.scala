package homelab.keyedqueue.domain.service.usecase.v1

import zio.Duration


/**
 * The synchronous API's apply cases behind a single dependency.
 *
 * One field per operation, and the set is the API: the four calls of `docs/research/phase-1-api.md`. When
 * streaming arrives it gets its own aggregate rather than growing this one, because the two speak different
 * request shapes and will not share a version.
 *
 * @param enqueue accepts a message for a key
 * @param dequeue waits for one
 * @param settle reports what a consumer did with it
 * @param heartbeat renews what a consumer still holds
 */
final case class SyncUseCases(
  enqueue: EnqueueUseCase,
  dequeue: DequeueUseCase,
  settle: SettleUseCase,
  heartbeat: HeartbeatUseCase,
)


object SyncUseCases:

  /**
   * The limits these apply cases enforce, expressed where they are enforced.
   *
   * A domain type rather than a reach into the service's configuration: the rule *"a caller may not wait
   * longer than this"* belongs to the apply case, while where the number comes from — a HOCON file, a flag, a
   * test — belongs to the adapter that provides it.
   *
   * @param maxWait the longest a caller may ask to wait for a message
   */
  final case class Config(maxWait: Duration, maxBatchLimit: Int)
