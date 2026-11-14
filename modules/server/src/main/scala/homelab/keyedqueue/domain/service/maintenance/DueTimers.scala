package homelab.keyedqueue.domain.service.maintenance

/**
 * The two timers of an instance, one per kind of due work, as one dependency.
 *
 * Bundled because two [[DueTimer]]s cannot sit in the environment under the same type: each mover is woken by its
 * own, and each use case that creates due work asks the matching one. Holds no logic, like
 * [[homelab.keyedqueue.domain.service.usecase.lock.LockUseCases]].
 *
 * @param schedules wakes the [[ScheduleMover]]; asked by the schedule use case
 * @param backoffs wakes the [[BackoffMover]]; asked by the settle use case when a nack sets a backoff
 */
final case class DueTimers(schedules: DueTimer, backoffs: DueTimer)


object DueTimers:

  /**
   * How each timer fires.
   *
   * @param schedules the schedule timer's quantum, slack and backstop
   * @param backoffs the backoff timer's quantum, slack and backstop
   */
  final case class Config(schedules: DueTimer.Config, backoffs: DueTimer.Config)
