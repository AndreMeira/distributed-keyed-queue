import sbt._

/**
 * This repo's two ends of GitHub Packages: what it resolves from, and where it publishes to.
 *
 * Both halves are here because they share one credential and are asymmetric in a way that is easy to get
 * wrong. **Publishing** from CI needs no secret of its own — Actions' built-in `GITHUB_TOKEN` can write to
 * its own repo's registry. **Consuming** needs a *classic* PAT with `read:packages`, since GitHub Packages
 * serves Maven only to authenticated callers, public repo or not.
 *
 * The catch for this repo in particular: it does both at once. A release resolves the toolkit from
 * *another* repo's registry and publishes to its own, and sbt matches credentials by host — so a single sbt
 * invocation cannot use a different token for each. Hence release.yml gives each half its own STEP: the
 * tests run under the read PAT, `publish` under the built-in token. Were they one step, the PAT would have
 * to carry `write:packages` as well.
 *
 * NOTE: this is Scala 2.12 sbt-DSL code (`import sbt._`, not `sbt.*`), like every file under `project/`.
 */
object GitHubPackages {

  /** This repo's own registry — where `publish` writes, and where a consumer of the contract resolves from. */
  val registry: MavenRepository =
    "GitHub Packages" at "https://maven.pkg.github.com/AndreMeira/distributed-keyed-queue"

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
