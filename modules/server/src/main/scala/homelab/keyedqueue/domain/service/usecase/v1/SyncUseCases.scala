package homelab.keyedqueue.domain.service.usecase.v1


/**
 * The synchronous API's use cases behind a single dependency.
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
