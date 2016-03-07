import sbtassembly.MergeStrategy
import sbtrelease.ReleasePlugin._

name := "cromwell"
version := "0.19"

// Name of the aggregate fat jar, sub-projects define their own in their build.sbt
assemblyJarName in assembly := "cromwell-" + version.value + ".jar"

// Dependency versions used in multiple places (multiple dependencies and/or multiple projects)
val lenthallV = "0.16"
val wdl4sV = "0.4-39df7ac-SNAPSHOT"
val sprayV = "1.3.2"
val DowngradedSprayV = "1.3.1"
val akkaV = "2.3.12"
val slickV = "3.1.1"
val googleClientApiV = "1.20.0"

// Common dependencies shared between multiple projects
// TODO: Currently this is empty, will immediately grow the moment we add code to cromwell-backend
// TODO: The test stuff will almost surely need to be here once MainSpec moves
val commonDependencies = List(
)

// TODO: It is expected that multiple engineDeps will wind up in commonDeps very rapidly
val engineDependencies = List(
  "org.broadinstitute" %% "lenthall" % lenthallV,
  "org.broadinstitute" %% "wdl4s" % wdl4sV,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "org.joda" % "joda-convert" % "1.8.1",
  "org.webjars" % "swagger-ui" % "2.1.1",
  "io.spray" %% "spray-can" % sprayV,
  "io.spray" %% "spray-routing" % sprayV,
  "io.spray" %% "spray-client" % sprayV,
  "io.spray" %% "spray-http" % sprayV,
  "io.spray" %% "spray-json" % DowngradedSprayV,
  "com.typesafe.akka" %% "akka-actor" % akkaV,
  "com.typesafe.akka" %% "akka-slf4j" % akkaV,
  "commons-codec" % "commons-codec" % "1.10",
  "commons-io" % "commons-io" % "2.4",
  "org.apache.commons" % "commons-lang3" % "3.4",
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "ch.qos.logback" % "logback-access" % "1.1.3",
  "org.codehaus.janino" % "janino" % "2.7.8",
  "com.typesafe.slick" %% "slick" % slickV,
  "com.typesafe.slick" %% "slick-hikaricp" % slickV,
  "org.hsqldb" % "hsqldb" % "2.3.2",
  "com.google.gcloud" % "gcloud-java" % "0.0.9",
  "com.google.api-client" % "google-api-client-java6" % googleClientApiV,
  "com.google.api-client" % "google-api-client-jackson2" % googleClientApiV,
  "com.google.oauth-client" % "google-oauth-client" % googleClientApiV,
  "com.google.cloud.bigdataoss" % "gcsio" % "1.4.3",
  "com.google.apis" % "google-api-services-genomics" % "v1alpha2-rev7-1.19.1",
  "mysql" % "mysql-connector-java" % "5.1.36",
  "org.scalaz" % "scalaz-core_2.11" % "7.1.3",
  "com.github.pathikrit" %% "better-files" % "2.13.0",
  "org.liquibase" % "liquibase-core" % "3.4.2",
  // This is to stop liquibase from being so noisy by default
  // See: http://stackoverflow.com/questions/20880783/how-to-get-liquibase-to-log-using-slf4j
  "com.mattbertolini" % "liquibase-slf4j" % "2.0.0",
  //---------- Test libraries -------------------//
  "io.spray" %% "spray-testkit" % sprayV % Test,
  "org.scalatest" %% "scalatest" % "2.2.5" % Test,
  "com.typesafe.akka" %% "akka-testkit" % akkaV % Test,
  "org.yaml" % "snakeyaml" % "1.16" % Test
)

// Kamon stuff
val kamonV = "0.5.2"
val kamonDependencies = List(
  "io.kamon" %% "kamon-core" % kamonV,
  "io.kamon" %% "kamon-akka" % kamonV,
  "io.kamon" %% "kamon-spray" % kamonV,
  "io.kamon" %% "kamon-system-metrics" % kamonV,
  "io.kamon" %% "kamon-statsd" % kamonV,
  "org.aspectj" % "aspectjweaver" % "1.8.6")

// Test configuration
lazy val AllTests = config("alltests") extend Test
lazy val NoTests = config("notests") extend Test
lazy val DockerTest = config("docker") extend Test
lazy val NoDockerTest = config("nodocker") extend Test
lazy val CromwellIntegrationTest = config("integration") extend Test
lazy val CromwellNoIntegrationTest = config("nointegration") extend Test

/*
  The arguments that will be added to the default test config, but removed from all other configs.
  `sbt coverage test` adds other arguments added to generate the coverage reports.
  Tracking the arguments we add to the default allows one to later remove them when building up other configurations.
 */
lazy val defaultTestArgs = Seq(Tests.Argument("-l", "DockerTest"), Tests.Argument("-l", "CromwellIntegrationTest"))

// `test` (or `assembly`) - Run all tests, except docker and integration
testOptions in Test ++= defaultTestArgs
// `alltests:test` - Run all tests
testOptions in AllTests := (testOptions in Test).value.diff(defaultTestArgs)
// `docker:test` - Run docker tests, except integration
testOptions in DockerTest := (testOptions in Test).value.diff(defaultTestArgs) ++
  Seq(Tests.Argument("-n", "DockerTest"), Tests.Argument("-l", "CromwellIntegrationTest"))
// `nodocker:test` - Run all tests, except docker
testOptions in NoDockerTest := (testOptions in Test).value.diff(defaultTestArgs) ++
  Seq(Tests.Argument("-l", "DockerTest"))
// `integration:test` - Run integration tests, except docker
testOptions in CromwellIntegrationTest := (testOptions in Test).value.diff(defaultTestArgs) ++
  Seq(Tests.Argument("-l", "DockerTest"), Tests.Argument("-n", "CromwellIntegrationTest"))
// `nointegration:test` - Run all tests, except integration
testOptions in CromwellNoIntegrationTest := (testOptions in Test).value.diff(defaultTestArgs) ++
  Seq(Tests.Argument("-l", "CromwellIntegrationTest"))
// `notests:assembly` - Disable all tests during assembly
/*
  TODO: This syntax of test in (NoTests, assembly) isn't correct

  Trying to get:

    sbt notests:assembly

  To be the same as:

    sbt 'set test in assembly := {}' assembly

  For now, one must use the more verbose command line until someone can crack sbt's custom configs/tasks/scopes for the
  assembly plugin:

    http://www.scala-sbt.org/0.13/tutorial/Scopes.html
 */
//test in (NoTests, assembly) := {}

// Also tried
//test in (NoTests, assemblyPackageDependency) := {}
//test in (NoTests, assemblyPackageScala) := {}

// Project definitions
// Settings common between all projects
// The reason why -Xmax-classfile-name is set on scalacOptions is because this will fail
// to build on Docker otherwise.  The reason why it's 200 is because it
// fails if the value is too close to 256 (even 254 fails).  For more info:
//
// https://github.com/sbt/sbt-assembly/issues/69
// https://github.com/scala/pickling/issues/10
val commonSettings = List(
  organization := "org.broadinstitute",
  scalaVersion := "2.11.7",
  scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-Xmax-classfile-name", "200"),
  resolvers ++= Seq(
     "Broad Artifactory Releases" at "https://artifactory.broadinstitute.org/artifactory/libs-release/",
     "Broad Artifactory Snapshots" at "https://artifactory.broadinstitute.org/artifactory/libs-snapshot/"),
  parallelExecution := false,
  libraryDependencies ++= commonDependencies
//  logLevel in assembly := Level.info
) ++ releaseSettings

// Cromwell Backend
lazy val backend = Project(id = "CromwellBackend", base = file("backend"))
  .settings(commonSettings: _*)

lazy val engine = Project(id = "CromwellEngine", base = file("engine"))
  .aggregate(backend)
  .dependsOn(backend)
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= engineDependencies)
  .settings(libraryDependencies ++= kamonDependencies)
  .configs(AllTests).settings(inConfig(AllTests)(Defaults.testTasks): _*)
  .configs(NoTests).settings(inConfig(NoTests)(Defaults.testTasks): _*)
  .configs(DockerTest).settings(inConfig(DockerTest)(Defaults.testTasks): _*)
  .configs(NoDockerTest).settings(inConfig(NoDockerTest)(Defaults.testTasks): _*)
  .configs(CromwellIntegrationTest).settings(inConfig(CromwellIntegrationTest)(Defaults.testTasks): _*)
  .configs(CromwellNoIntegrationTest).settings(inConfig(CromwellNoIntegrationTest)(Defaults.testTasks): _*)

// Root project
// NOTE: The following block may cause problems with IntelliJ IDEA
// by creating multiple test configurations.
// May need to comment out when importing the project.
lazy val root = Project(id = "Cromwell", base = file("."))
  .aggregate(backend, engine)
  .dependsOn(backend, engine)
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= engineDependencies)
  .settings(libraryDependencies ++= kamonDependencies)

// Merge strategies and such
packageOptions in assembly += Package.ManifestAttributes("Premain-Class" -> "org.aspectj.weaver.loadtime.Agent")

// Create a new MergeStrategy for aop.xml files
val aopMerge: MergeStrategy = new MergeStrategy {
  val name = "aopMerge"
  import scala.xml._
  import scala.xml.dtd._
  def apply(tempDir: File, path: String, files: Seq[File]): Either[String, Seq[(File, String)]] = {
    val dt = DocType("aspectj", PublicID("-//AspectJ//DTD//EN", "http://www.eclipse.org/aspectj/dtd/aspectj.dtd"), Nil)
    val file = MergeStrategy.createMergeTarget(tempDir, path)
    val xmls: Seq[Elem] = files.map(XML.loadFile)
    val aspectsChildren: Seq[Node] = xmls.flatMap(_ \\ "aspectj" \ "aspects" \ "_")
    val weaverChildren: Seq[Node] = xmls.flatMap(_ \\ "aspectj" \ "weaver" \ "_")
    val options: String = xmls.map(x => (x \\ "aspectj" \ "weaver" \ "@options").text).mkString(" ").trim
    val weaverAttr = if (options.isEmpty) Null else new UnprefixedAttribute("options", options, Null)
    val aspects = new Elem(null, "aspects", Null, TopScope, false, aspectsChildren: _*)
    val weaver = new Elem(null, "weaver", weaverAttr, TopScope, false, weaverChildren: _*)
    val aspectj = new Elem(null, "aspectj", Null, TopScope, false, aspects, weaver)
    XML.save(file.toString, aspectj, "UTF-8", xmlDecl = false, dt)
    IO.append(file, IO.Newline.getBytes(IO.defaultCharset))
    Right(Seq(file -> path))
  }
}

val customMergeStrategy: String => MergeStrategy = {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", "aop.xml") =>
    aopMerge
  case PathList("META-INF", path@_*) =>
    path map {
      _.toLowerCase
    } match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps@(x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "spring.tooling" :: xs =>        
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.deduplicate
    }
  case "asm-license.txt" | "overview.html" | "cobertura.properties" =>
    MergeStrategy.discard
  case _ => MergeStrategy.deduplicate
}

assemblyMergeStrategy in assembly := customMergeStrategy