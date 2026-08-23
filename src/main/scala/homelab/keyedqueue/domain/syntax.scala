package homelab.keyedqueue.domain


import homelab.keyedqueue.domain.error.{ InvalidInput, QueueError }
import zio.{ IO, NonEmptyChunk, ZIO }


/** Domain-level aliases and the syntax that goes with them. */
object syntax:

  /**
   * A validation in progress: it may fail with input problems, and failures '''accumulate'''.
   *
   * The accumulation is ZIO's own rather than a `Validation` type from another library. `ZIO#validate` runs
   * both sides and, when both fail, combines their causes — so every failure is still there in the `Cause`
   * when the validation is finally collapsed by [[orFail]]. Same property as an applicative `Validation`, in
   * the effect type the rest of the domain already speaks.
   *
   * '''It stays accumulating until somebody collapses it.''' That is deliberate: a validator that failed fast
   * on its caller's behalf could never be composed with another validator, which is the whole point.
   */
  type Validated[A] = IO[InvalidInput, A]

  extension [A](validated: Validated[A])

    /**
     * Stop accumulating: give the value, or fail with everything that was wrong.
     *
     * The moment a validation becomes a refusal, and it belongs to the *caller* — a use case — rather than to
     * the validators, which have to stay composable. An extension rather than a function because it reads at
     * the call site as the last step of a validation (`validation.enqueue(request).orFail`) instead of
     * wrapping it.
     *
     * The problems are read out of the `Cause`, which is where `ZIO#validate` leaves them when several checks
     * fail. A cause carrying no typed failures is a defect or an interruption rather than a validation
     * result, so it is re-raised untouched instead of being reported to a caller as their mistake.
     *
     * @return the validated value; aborts with `InvalidRequest` naming every problem found
     */
    def orFail: IO[QueueError, A] =
      validated.catchAllCause: cause =>
        NonEmptyChunk.fromIterableOption(cause.failures) match
          case Some(problems) => ZIO.fail(QueueError.InvalidRequest(problems))
          case None           => ZIO.refailCause(cause.stripFailures)
