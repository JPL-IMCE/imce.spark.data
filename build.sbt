import sbt.Keys._
import sbt._

import gov.nasa.jpl.imce.sbt._
import gov.nasa.jpl.imce.sbt.ProjectHelper._

updateOptions := updateOptions.value.withCachedResolution(true)

lazy val core = Project("imce-spark-data", file("."))
  .enablePlugins(IMCEGitPlugin)
  .enablePlugins(IMCEReleasePlugin)
  .settings(IMCEPlugin.strictScalacFatalWarningsSettings)
  .settings(
    IMCEKeys.licenseYearOrRange := "2016",
    IMCEKeys.organizationInfo := IMCEPlugin.Organizations.omf,

    buildInfoPackage := "imce.spark.data",
    buildInfoKeys ++= Seq[BuildInfoKey](BuildInfoKey.action("buildDateUTC") { buildUTCDate.value }),

    projectID := {
      val previous = projectID.value
      previous.extra(
        "build.date.utc" -> buildUTCDate.value,
        "artifact.kind" -> "generic.library")
    },

    IMCEKeys.targetJDK := IMCEKeys.jdk18.value,
    git.baseVersion := Versions.version,
    // include all test artifacts
    publishArtifact in Test := true,

    scalaSource in Test := baseDirectory.value / "test",

    libraryDependencies ++= Seq (
      "org.apache.spark" %% "spark-core" % "2.0.1",
      "org.apache.spark" %% "spark-sql" % "2.0.1"
    ),

    extractArchives := {},

    resolvers += Resolver.bintrayRepo("jpl-imce", "gov.nasa.jpl.imce"),
    resolvers += Resolver.bintrayRepo("tiwg", "org.omg.tiwg")
  )
  .dependsOnSourceProjectOrLibraryArtifacts(
    "jpl-omf-schema-tables",
    "jpl.omf.schema.tables",
    Seq(
      "gov.nasa.jpl.imce" %% "jpl-omf-schema-tables"
        % Versions_omf_schema_tables.version
    )
  )
