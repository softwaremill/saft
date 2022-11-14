import com.softwaremill.SbtSoftwareMillCommon.commonSmlBuildSettings

val sttpVersion = "3.8.3"
val zioJsonVersion = "0.3.0"

lazy val commonSettings = commonSmlBuildSettings ++ Seq(
  organization := "com.softwaremill.saft",
  scalaVersion := "3.2.1"
)

lazy val rootProject = (project in file("."))
  .settings(commonSettings: _*)
  .settings(publishArtifact := false, name := "saft")
  .aggregate(zio, loom)

lazy val zio: Project = (project in file("zio"))
  .settings(commonSettings: _*)
  .settings(
    name := "zio",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.3",
      "dev.zio" %% "zio-logging" % "2.1.4",
      "io.d11" %% "zhttp" % "2.0.0-RC11",
      "dev.zio" %% "zio-json" % zioJsonVersion,
      "com.softwaremill.sttp.client3" %% "zio" % sttpVersion,
      "dev.zio" %% "zio-test" % "2.0.3" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.0.3" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val loom: Project = (project in file("loom"))
  .settings(commonSettings: _*)
  .settings(
    name := "loom",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.4.4",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "dev.zio" %% "zio-json" % zioJsonVersion,
      "org.eclipse.jetty" % "jetty-server" % "11.0.12",
      "org.scalatest" %% "scalatest" % "3.2.14" % Test
    ),
    javaOptions += "--enable-preview --add-modules jdk.incubator.concurrent",
    run / fork := true
  )
