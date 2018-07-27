import sbt._
import Keys._

object Settings {
  lazy val settings = Seq(
    organization := "com.lendap",
    version := "2.0." + sys.props.getOrElse("buildNumber", default="0-SNAPSHOT"),
    scalaVersion := "2.11.8",
    publishMavenStyle := true,
    publishArtifact in Test := false
  )

  lazy val testSettings = Seq(
    fork in Test := false,
    parallelExecution in Test := false
  )

  lazy val lshSettings = Seq(
    name := "lsh-spark"
  )
}
