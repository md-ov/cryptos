import sbt.Resolver

name := "sryptosbt"

version := "0.1"

scalaVersion := "2.12.6"

resolvers += Resolver.sonatypeRepo("releases")
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"
resolvers += Resolver.mavenLocal
resolvers += Resolver.mavenCentral

libraryDependencies += "com.github.alexarchambault" %% "case-app" % "2.0.0-M3"

enablePlugins(PackPlugin)
