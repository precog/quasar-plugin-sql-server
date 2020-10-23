ThisBuild / crossScalaVersions := Seq("2.12.10")
ThisBuild / scalaVersion := (ThisBuild / crossScalaVersions).value.head

ThisBuild / githubRepository := "quasar-plugin-sql-server"

ThisBuild / homepage := Some(url("https://github.com/precog/quasar-plugin-sql-server"))

ThisBuild / scmInfo := Some(ScmInfo(
  url("https://github.com/precog/quasar-plugin-sql-server"),
  "scm:git@github.com:precog/quasar-plugin-sql-server.git"))

ThisBuild / publishAsOSSProject := true

lazy val quasarVersion =
  Def.setting[String](managedVersions.value("precog-quasar"))

lazy val quasarPluginJdbcVersion =
  Def.setting[String](managedVersions.value("precog-quasar-plugin-jdbc"))

val specs2Version = "4.9.4"

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  Test / packageBin / publishArtifact := true)

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core, datasource)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "quasar-plugin-sql-server",
    libraryDependencies ++= Seq(
      "com.precog"     %% "quasar-plugin-jdbc"         % quasarPluginJdbcVersion.value,
      "com.codecommit" %% "cats-effect-testing-specs2" % "0.4.0"       % Test,
      "org.specs2"     %% "specs2-core"                % specs2Version % Test))

lazy val datasource = project
  .in(file("datasource"))
  .dependsOn(core % BothScopes)
  .settings(
    name := "quasar-datasource-sql-server",

    quasarPluginName := "sql-server",
    quasarPluginQuasarVersion := quasarVersion.value,
    quasarPluginDatasourceFqcn := Some("quasar.plugin.sqlserver.datasource.SQLServerDatasourceModule$"),

    quasarPluginDependencies ++= Seq(
      "com.precog"              %% "quasar-plugin-jdbc" % quasarPluginJdbcVersion.value,
      "com.microsoft.sqlserver" %  "mssql-jdbc"         % "8.4.1.jre8"
    ))
  .enablePlugins(QuasarPlugin)
