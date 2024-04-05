//Generate JaCoCo reports for test coverage.
addSbtPlugin("com.github.sbt" % "sbt-jacoco" % "3.4.0")

//scalastyle support for sbt.
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

//sbt-scoverage is a plugin for SBT that integrates the scoverage code coverage library.
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.8")

//SonarQube Integration
addSbtPlugin("com.sonar-scala" % "sbt-sonar" % "2.3.0")

//For generating build info
addSbtPlugin("com.eed3si9n" % "sbt-assembly"  % "2.1.1")

//For GitHub API documentation publishing
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.3")
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")

addDependencyTreePlugin

ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)