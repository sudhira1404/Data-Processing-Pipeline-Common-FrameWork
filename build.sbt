import sbt.ExclusionRule

name := "mdf_dpp_common"
val envVersion = sys.env.getOrElse("build_version", "0.1")
lazy val scala212 = sys.env.getOrElse("scala212","2.12.18")
lazy val scala211 = sys.env.getOrElse("scala211","2.11.12")
lazy val supportedScalaVersions = List(scala212, scala211)
lazy val spark2 = sys.env.getOrElse("spark2","2.3.1.tgt.26")
lazy val spark3 = sys.env.getOrElse("spark3","3.4.1")

ThisBuild / organization := "com.tgt.dse.mdf.common.pipeline"
ThisBuild / version := envVersion
ThisBuild / scalaVersion := scala212

val publishMavenStyle = true

/**
 * Repositories where the dependencies will be pulled from.
 */
resolvers := Seq(
  "BigRed Artifactory Repo"         at "https://binrepo.target.com/artifactory/bigRED",
  "repo1-cache Artifactory Repo"    at "https://binrepo.target.com/artifactory/repo1-cache",
  "jcenter-cache Artifactory Repo"  at "https://binrepo.target.com/artifactory/jcenter-cache",
  "Kelsa Artifactory"               at "https://binrepo.target.com/artifactory/kelsa",
  "MDF Artifactory" at "https://binrepo.target.com/artifactory/mdf",
  "maven-central-cache Artifactory Repo" at "https://binrepo.target.com/artifactory/maven-central-cache",
  "SharedObjects" at "https://binrepo.target.com/artifactory/dse-sharedobjects/"
)

Test / fork :=true
//Java options for test
javaOptions ++= Seq("-Xms512M","-Xmx2G","-XX:+CMSClassUnloadingEnabled")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case x => MergeStrategy.first
}

Test / parallelExecution := true
/**
 * coverage stuff
 */
coverageHighlighting := true

//Scoverage settings
coverageMinimumStmtTotal := 70
coverageFailOnMinimum := true

Test / jacocoExcludes := Seq(
  "*.SftpTransferService*",
  "*.SftpTransferConfig*",
  "*.SftpUtils*",
  "*.CredentialUtils*",
  "*.EmailService*",
  "*.HttpService*",
  "*.CommonExecutePipelineWrapper*",
  "*.CommonExecutePipelineParameterObjects*"
)

// jacoco settings
jacocoReportSettings := JacocoReportSettings(
  title = name.value,
  formats = Seq(
    JacocoReportFormats.ScalaHTML,
    JacocoReportFormats.CSV,
    JacocoReportFormats.XML
  )
)
  .withThresholds(
    JacocoThresholds(
      line = 70
    )
  )

//sonar settings
val sonar_token = sys.env.getOrElse("SONAR_TOKEN", "")
sonarProperties ++= Map(
  "sonar.host.url" -> "https://desonar.prod.target.com",
  "sonar.projectKey" -> "data-engineering-mktg-data-foundation.mdf_dpp_common",
  "sonar.projectName" -> "data-engineering-mktg-data-foundation.mdf_dpp_common",
  "sonar.verbose" -> "true",
  "sonar.login" -> sonar_token
)

doc / scalacOptions ++= Seq("-no-link-warnings") // Suppresses problems with Scaladoc @throws links)

enablePlugins(GhpagesPlugin)
enablePlugins(SiteScaladocPlugin)
lazy val buildVersion = sys.env.getOrElse("build_version","latest")
SiteScaladoc / siteSubdirName := buildVersion
ghpagesCommitOptions := Seq("-m", s"add documentation for $buildVersion [SEDATED FALSE POSITIVE]")
git.remoteRepo := "git@git.target.com:data-engineering-mktg-data-foundation/mdf_dpp_common.git"
ghpagesCleanSite / excludeFilter := "*.*.*"

lazy val core = (project in file("."))
  .settings(
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= {
      CrossVersion.binaryScalaVersion(scalaVersion.value) match {
        case "2.11" =>
          Seq(
            "org.scalamock" %% "scalamock" % "4.4.0" % Test,
            "com.typesafe" % "config" % "1.3.3",
            "org.slf4j" % "slf4j-api" % "1.7.36",
            "org.slf4j" % "slf4j-log4j12" % "1.7.36",
            "org.scalatest" %% "scalatest" % "3.1.0" % Test,
            "joda-time" % "joda-time" % "2.1",
            "org.joda" % "joda-convert" % "1.2",
            "org.apache.spark" %% "spark-core" % spark2 % Provided,
            "org.apache.spark" %% "spark-sql" % spark2 % Provided,
            "org.apache.spark" %% "spark-hive" % spark2 % Provided,
            "org.apache.kafka" % "kafka-clients" % "3.3.1",
            "org.scala-lang" % "scala-library" % scala211,
            "com.tgt.dsc.kelsa.datapipeline" %% "kelsa-core" % "1.8.4",
            "org.scoverage" % s"scalac-scoverage-plugin_${scala211}" % "1.4.11" % Provided,
            "com.springml"           % "sftp.client"     % "1.0.3",
            //"com.jcraft" % "jsch" % "0.1.55" % Provided,
            "com.sun.mail"  % "javax.mail" % "1.6.2",
            "com.github.mwiede" % "jsch" % "0.2.16"
          )
        case "2.12" =>
          Seq(
            "org.scalamock" %% "scalamock" % "5.2.0" % Test,
            "com.typesafe" % "config" % "1.4.2",
            "org.slf4j" % "slf4j-api" % "2.0.5",
            "org.slf4j" % "slf4j-reload4j" % "2.0.5",
            "org.scalatest" %% "scalatest" % "3.2.15" % Test,
            "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
            "org.apache.spark" %% "spark-core" % spark3 % Provided,
            "org.apache.spark" %% "spark-sql" % spark3 % Provided,
            "org.apache.spark" %% "spark-hive" % spark3 % Provided,
            "org.apache.hadoop" % "hadoop-auth" % "3.3.5" % Provided,
            "org.apache.hadoop" % "hadoop-common" % "3.3.5" % Provided,
            "org.apache.kafka" % "kafka-clients" % "3.5.0" exclude("org.xerial.snappy","snappy-java"),//That'll make SCA happy
            "org.scala-lang" % "scala-library" % scala212,
            "com.tgt.dsc.kelsa.datapipeline" %% "kelsa-core" % "2.0" % Provided,
            "com.springml"           % "sftp.client"     % "1.0.3",
            //"com.jcraft" % "jsch" % "0.1.55" % Provided,
            "com.sun.mail"  % "javax.mail" % "1.6.2",
            "com.github.mwiede" % "jsch" % "0.2.16"
          )
        case _ => Nil
      }
    },
    dependencyOverrides ++= {
      CrossVersion.binaryScalaVersion(scalaVersion.value) match {
        case "2.11" =>
          Seq ("com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7")
        case "2.12" =>
          Seq (
            "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
            "org.apache.logging.log4j" % "log4j-1.2-api" % "2.20.0",
            "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
            "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
            "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0",
            "org.slf4j" % "jcl-over-slf4j" % "2.0.5",
            "org.slf4j" % "jul-to-slf4j" % "2.0.5"
          )
      }
    },
    excludeDependencies ++= {
      CrossVersion.binaryScalaVersion(scalaVersion.value) match {
        case "2.11" =>
          Seq(ExclusionRule("log4j","log4j")
            ,ExclusionRule("com.jcraft","jsch")
         )
        case "2.12" =>
          Seq(
            ExclusionRule("log4j","log4j"),
            ExclusionRule("org.slf4j","slf4j-log4j12")
              ,ExclusionRule("com.jcraft","jsch")
          )
      }
    },
    sonarProperties ++= {
      CrossVersion.binaryScalaVersion(scalaVersion.value) match {
        case "2.11" =>
          Map(
            "sonar.scala.version" -> "2.11",
            "sonar.scala.scoverage.reportPath" -> "target/scala-2.11/scoverage-report/scoverage.xml",
            "sonar.scala.scapegoat.reportPath" -> "target/scala-2.11/scapegoat-report/scapegoat.xml",
            "sonar.coverage.jacoco.xmlReportPaths" -> "target/scala-2.11/jacoco/report/jacoco.xml"
          )
        case "2.12" =>
          Map(
            "sonar.scala.version" -> "2.12",
            "sonar.scala.scoverage.reportPath" -> "target/scala-2.12/scoverage-report/scoverage.xml",
            "sonar.scala.scapegoat.reportPath" -> "target/scala-2.12/scapegoat-report/scapegoat.xml",
            "sonar.coverage.jacoco.xmlReportPaths" -> "target/scala-2.12/jacoco/report/jacoco.xml"
          )
      }
    },
    coverageScalacPluginVersion := {
      CrossVersion.binaryScalaVersion(scalaVersion.value) match {
        case "2.11" => "1.4.1"
        case "2.12" => "2.0.10"
      }
    },
    javacOptions ++= {
      CrossVersion.binaryScalaVersion(scalaVersion.value) match {
        case "2.11" => Seq("-source", "1.8")
        case "2.12" => Seq("-source", "11")
      }
    },
    scalacOptions ++= {
      CrossVersion.binaryScalaVersion(scalaVersion.value) match {
        case "2.11" => Seq("-target:jvm-1.8")
        case "2.12" => Seq("-target:11")
      }
    }
  )