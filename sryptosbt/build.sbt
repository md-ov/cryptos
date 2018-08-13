import sbt.Resolver

name := "sryptosbt"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += Resolver.sonatypeRepo("releases")
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"
resolvers += Resolver.mavenLocal
resolvers += Resolver.mavenCentral

libraryDependencies += "com.github.alexarchambault" %% "case-app" % "2.0.0-M3"

libraryDependencies ++= Seq('mllib,
    'core,
    'sql,
    'hive).map(c => "org.apache.spark" %% s"spark-${c.name}" % "2.3.1")

enablePlugins(PackPlugin)
