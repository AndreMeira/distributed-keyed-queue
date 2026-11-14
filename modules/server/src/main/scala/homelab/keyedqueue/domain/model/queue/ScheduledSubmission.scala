package homelab.keyedqueue.domain.model.queue

import zio.Duration

/**
 * A message accepted for a queue, to be appended to its key's line after a delay.
 *
 * Obtainable only from the parse, like a [[Submission]], so the delay is known to be positive and within the
 * horizon by the time a store sees it. Named `ScheduledSubmission` rather than `Schedule`, which every file
 * importing `zio.*` would confuse with `zio.Schedule`.
 *
 * @param submission where it goes, and what it is
 * @param delay how long from now it becomes claimable, measured on the store's clock
 */
final case class ScheduledSubmission(submission: Submission, delay: Duration)
