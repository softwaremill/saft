import com.softwaremill.SbtSoftwareMillCommon.commonSmlBuildSettings

lazy val commonSettings = commonSmlBuildSettings ++ Seq(
  organization := "com.softwaremill.saft",
  scalaVersion := "3.1.3"
)

lazy val rootProject = (project in file("."))
  .settings(commonSettings: _*)
  .settings(publishArtifact := false, name := "saft")
  .aggregate(zio)

lazy val zio: Project = (project in file("zio"))
  .settings(commonSettings: _*)
  .settings(
    name := "zio",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.1",
      "dev.zio" %% "zio-logging" % "2.1.0",
      "io.d11" %% "zhttp" % "2.0.0-RC11",
      "dev.zio" %% "zio-json" % "0.3.0-RC11",
      "com.softwaremill.sttp.client3" %% "zio" % "3.7.6",
      "dev.zio" %% "zio-test" % "2.0.1" % Test,
      "dev.zio" %% "zio-test-sbt" % "2.0.1" % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
