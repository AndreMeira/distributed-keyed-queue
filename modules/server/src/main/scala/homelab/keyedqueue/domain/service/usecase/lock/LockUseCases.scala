package homelab.keyedqueue.domain.service.usecase.lock

import homelab.keyedqueue.domain.service.usecase.lock.{ LockAcquireUseCase, LockRefreshUseCase, LockReleaseUseCase }
import homelab.keyedqueue.domain.service.usecase.queue.QueueUseCases


/**
 * The lock API's use cases behind a single dependency — the lock's [[QueueUseCases]].
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
