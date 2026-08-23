package homelab.keyedqueue.domain.service.validation


import homelab.keyedqueue.domain.error.InvalidInput
import homelab.keyedqueue.domain.syntax.Validated
import zio.ZIO


/**
 * The building blocks every validator composes.
 *
 * Pure — no store, no clock, no configuration — so it needs no wiring and can be called from anywhere in the
 * domain. Stateful checks (does this queue exist, is this key parked) would be a class with dependencies,
 * wired through [[Module]]; there are none yet.
 */
object CommonValidation:

  /**
   * Require a value to carry something.
   *
   * Takes `A <: String` rather than `String` so it works directly on the domain's opaque types and hands the
   * *same type* back — a check that returned a bare `String` would force every caller to reconstruct its
   * value object, which is where a wrong one gets built.
   *
   * @param value the value to check
   * @param problem what to report when it is empty
   * @tparam A the value's type, some kind of string
   * @return the value; fails with `problem` when it is empty
   */
  def nonEmpty[A <: String](value: A, problem: InvalidInput): Validated[A] =
    if value.isEmpty then ZIO.fail(problem) else ZIO.succeed(value)
