package homelab.keyedqueue.domain.service.usecase.lock


/**
 * The lock API's use cases behind a single dependency — the lock's [[QueueUseCases]].
 *
 * @param acquire takes a lock, waiting for it
 * @param tryAcquire takes one only if it is free now
 * @param release frees one the caller holds
 * @param refresh extends a held lock's lease
 */
final case class LockUseCases(
  acquire: LockAcquireUseCase,
  tryAcquire: LockTryAcquireUseCase,
  release: LockReleaseUseCase,
  refresh: LockRefreshUseCase,
)
