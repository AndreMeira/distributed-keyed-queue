package homelab.keyedqueue.domain.service.usecase.v1


/**
 * The lock API's use cases behind a single dependency — the lock's [[SyncUseCases]].
 *
 * @param acquire takes a lock, waiting for it
 * @param release frees one the caller holds
 * @param refresh extends a held lock's lease
 */
final case class SyncLockUseCases(
  acquire: LockAcquireUseCase,
  release: LockReleaseUseCase,
  refresh: LockRefreshUseCase,
)
