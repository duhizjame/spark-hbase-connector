ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.11"

val sparkVersion = "3.2.1"

val sparkDep = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion)

val hbaseDep = Seq(
  "org.apache.hbase" % "hbase-client" % "2.5.3" excludeAll(
    ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-common"),
    ExclusionRule(organization = "org.apache.hadoop", name = "hadoop-auth") // intransitive(),
  )
)

val testDep = Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
)

lazy val root = (project in file("."))
  .settings(
    name := "spark-hbase-connector",
    libraryDependencies ++= sparkDep,
    libraryDependencies ++= hbaseDep,
    libraryDependencies ++= testDep,
    idePackagePrefix := Some("am")
  )
