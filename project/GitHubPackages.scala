import sbt._

/**
 * Resolving `com.andremeira.homelab` artifacts from GitHub Packages.
 *
 * GitHub Packages serves Maven only to authenticated callers — public repo or not, and only to a *classic*
 * PAT. So a consumer needs two things, and this object is both of them in one place, out of `build.sbt`.
 *
 * NOTE: this is Scala 2.12 sbt-DSL code (`import sbt._`, not `sbt.*`), like every file under `project/`.
 */
object GitHubPackages {

  /** The registry holding the homelab toolkit's modules. */
  val toolkit: MavenRepository =
    "homelab-toolkit-zio" at "https://maven.pkg.github.com/AndreMeira/homelab-toolkit-zio"

  /**
   * Credentials from the environment in CI (`GITHUB_ACTOR` / `GITHUB_TOKEN`) and from
   * `~/.sbt/1.0/credentials` on a laptop — sbt does not read that file unless a build asks it to. Empty
   * when neither exists, so a build with no credentials fails on the 401 rather than on a missing file.
   *
   * The realm is fixed by GitHub. Get it wrong and sbt silently skips these, which surfaces as a 401 that
   * reads like a bad token.
   */
  def credentials: Seq[Credentials] =
    (sys.env.get("GITHUB_ACTOR"), sys.env.get("GITHUB_TOKEN")) match {
      case (Some(actor), Some(token)) =>
        Seq(Credentials("GitHub Package Registry", "maven.pkg.github.com", actor, token))
      case _ =>
        val local = Path.userHome / ".sbt" / "1.0" / "credentials"
        if (local.exists) Seq(Credentials(local)) else Nil
    }
}
