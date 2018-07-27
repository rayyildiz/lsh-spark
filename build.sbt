scalaVersion := "2.11.8"

lazy val lsh = (project in file(".")).
  settings(Settings.settings: _*).
  settings(Settings.lshSettings: _*).
  settings(libraryDependencies ++=Dependencies.lshDependencies )