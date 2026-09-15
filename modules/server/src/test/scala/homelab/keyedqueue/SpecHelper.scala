package homelab.keyedqueue


import zio.durationInt
import zio.test.{ TestAspect, TestAspectPoly }


object SpecHelper {

  object Aspect {

    /**
     * The conditions a suite that needs sequential execution and timeout
     */
    val common: TestAspectPoly =
      TestAspect.withLiveClock >>> TestAspect.sequential >>> TestAspect.timeout(3.minutes)
  }
}
