name := "pushdown-datasource"

organization := ""
version := "0.1.0"
// githubOwner := "?"
// githubRepository := "pushdown-datasource"
// githubTokenSource := TokenSource.Environment("GITHUB_TOKEN")

// pushRemoteCacheConfiguration := pushRemoteCacheConfiguration.value.withOverwrite(true)
// publishConfiguration := publishConfiguration.value.withOverwrite(true)
// publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
scalaVersion := "2.12.10"

val sparkVersion = "3.0.0"

// We want to execute the tests serially.
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.11.434" exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.apache.commons" % "commons-csv" % "1.8",
  "org.apache.httpcomponents" % "httpcore" % "4.4.11",
  "org.slf4j" % "slf4j-api" % "1.7.30" % "provided",
  "org.mockito" % "mockito-core" % "2.0.31-beta",
 )
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M3"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.10" % "test"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
)
// Libraries for the ndp client.
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "3.2.0",
  "org.slf4j" % "slf4j-simple" % "1.7.21" % Test,
  "org.apache.logging.log4j" % "log4j-api" % "2.14.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.14.0",
)
// githubOwner := "?"
// githubRepository := "pushdown-datasource"
// credentials += Credentials(
//           "GitHub Package Registry",
//           "maven.pkg.github.com",
//           //sys.env.get("GITHUB_ACTOR").getOrElse("N/A"),
//          "?",
//          sys.env.getOrElse("GITHUB_TOKEN", "c164a97693d4694b8b4a05477f154dfc2ea0eafb")        
//        )
// githubTokenSource := Some(TokenSource.GitConfig("token"))


// val GlobalSettingsGroup: Seq[Setting[_]] = Seq(
//      githubOwner := "?",
//      githubRepository := "pushdown-datasource",
//      credentials +=
//        Credentials(
//          "GitHub Package Registry",
//          "maven.pkg.github.com",
//          sys.env.get("GITHUB_ACTOR").getOrElse("N/A"),
//          sys.env.getOrElse("GITHUB_TOKEN", "N/A")
//        ),
  //scala version, organization, version etc are also here
//)
