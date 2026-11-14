package homelab.keyedqueue.domain.service.usecase.queue


import homelab.common.error.ApplicationError
import homelab.common.orFail
import homelab.keyedqueue.domain.request.queue.ScheduleRequest
import homelab.keyedqueue.domain.response.queue.ScheduleResponse
import homelab.keyedqueue.domain.service.maintenance.{ DueTimer, Watchdog }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.model.queue.ScheduledSubmission
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import zio.{ IO, ZIO }


/**
 * Accept a message to be appended to its key's line after a delay.
 *
 * @param store where scheduled messages wait
 * @param watchdog told about the queue, so its abandoned work is repaired once the message is moved
 * @param validation what makes a request actionable
 * @param timer asked for a tick at the due time, so this instance moves the message on time
 */
final class QueueScheduleUseCase(store: QueueStore, watchdog: Watchdog, validation: QueueInputValidation, timer: DueTimer):

  /**
   * Parse, record, then ask for a tick at the due time.
   *
   * The tick is asked for whether this request was recorded or ignored: the store's answer is the recorded
   * schedule's due time either way, and asking again costs nothing while it re-arms this instance if the one
   * that took the first request has gone. The request is only a hint, made after the write it is about, so a
   * tick lost with this instance delays the message to another instance's backstop and loses nothing.
   *
   * @param request the queue, the message and the delay, untrusted
   * @return whether the schedule was recorded, and how long until it is due; aborts with `ValidationError`
   *         naming everything wrong with the request, or with `ApplicationError` when the store fails
   */
  def apply(request: ScheduleRequest): IO[ApplicationError, ScheduleResponse] =
    for
      scheduled <- validation.parse(request).orFail
      _         <- watchdog.watch(scheduled.submission.queue)
      recorded  <- record(scheduled)
    yield ScheduleResponse(recorded.scheduled, recorded.dueIn)

  /**
   * Record the schedule, then ask the timer for a tick at its due time, as one step a cancellation cannot split.
   *
   * Uninterruptible because a caller cancelled between the two would leave a schedule recorded with no tick
   * asked for, so it would wait for another pass (up to the backstop) while this instance is healthy, which S3
   * does not allow. A cancelled request may therefore still be recorded; that is harmless, since a retry of it is
   * ignored (the first schedule of an id wins). Nothing in the step blocks beyond the store call, which waits for
   * its reply uninterruptibly anyway and is bounded by the command timeout.
   *
   * @param scheduled what to record
   * @return what the store recorded; aborts with `ApplicationError` when the store fails
   */
  private def record(scheduled: ScheduledSubmission): IO[ApplicationError, QueueStore.Scheduled] =
    ZIO.uninterruptible:
      for
        recorded <- store.schedule(scheduled)
        _        <- timer.triggerIn(recorded.dueIn)
      yield recorded
