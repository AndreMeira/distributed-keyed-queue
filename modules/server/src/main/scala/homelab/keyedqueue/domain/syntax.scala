package homelab.keyedqueue.domain


import homelab.common.error.ValidationError
import homelab.keyedqueue.domain.error.QueueError
import zio.prelude.Validation
import zio.{ IO, ZIO }


/** Domain-level aliases and the syntax that goes with them. */
object syntax:

  /**
   * A validation in progress: it holds a value, or every problem found on the way to not having one.
   *
   * '''A value rather than an effect.''' Accumulation is what validation is for — a request with two
   * problems must report two — and an error channel cannot do it: `ZIO` short-circuits on the first failure
   * by construction. `Validation` is the applicative that combines failures instead, so validating two
   * checks that both fail yields both.
   *
   * Parameterised by the toolkit's [[ValidationError.InvalidInput]] rather than by this service's enum, so
   * a check written against the homelab's contract composes here unchanged. What the enum adds is the list
   * of problems a *queue* can have.
   *
   * '''It stays a value until somebody collapses it.''' That is deliberate: a validator that failed on its
   * caller's behalf could never be composed with another validator, which is the whole point.
   */
  type Validated[A] = Validation[ValidationError.InvalidInput, A]

  extension [A](validated: Validated[A])

    /**
     * Leave the applicative: give the value, or fail with everything that was wrong.
     *
     * The moment a validation becomes a refusal, and it belongs to the *caller* — a use case — rather than
     * to the validators, which have to stay composable. An extension rather than a function because it
     * reads at the call site as the last step of a validation (`validation.parse(request).orFail`) instead
     * of wrapping it.
     *
     * @return the validated value; aborts with `InvalidRequest` naming every problem found
     */
    def orFail: IO[QueueError, A] =
      ZIO.fromEither(validated.toEither).mapError(QueueError.InvalidRequest.apply)
