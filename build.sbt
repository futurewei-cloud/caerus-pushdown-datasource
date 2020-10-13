name := "s3datasource"

organization := ""
version := "0.1.0"
githubOwner := "rf972"
githubRepository := "s3datasource"
githubTokenSource := TokenSource.Environment("GITHUB_TOKEN")
pushRemoteCacheConfiguration := pushRemoteCacheConfiguration.value.withOverwrite(true)
publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
scalaVersion := "2.12.10"

val sparkVersion = "3.0.0"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.11.434" exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.apache.commons" % "commons-csv" % "1.8",
  "org.slf4j" % "slf4j-api" % "1.7.30" % "provided",
  "org.mockito" % "mockito-core" % "2.0.31-beta"
)

libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % sparkVersion,
 "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile",
)
// githubOwner := "rf972"
// githubRepository := "s3datasource"
// credentials += Credentials(
//           "GitHub Package Registry",
//           "maven.pkg.github.com",
//           //sys.env.get("GITHUB_ACTOR").getOrElse("N/A"),
//          "rf972",
//          sys.env.getOrElse("GITHUB_TOKEN", "c164a97693d4694b8b4a05477f154dfc2ea0eafb")        
//        )
// githubTokenSource := Some(TokenSource.GitConfig("token"))


// val GlobalSettingsGroup: Seq[Setting[_]] = Seq(
//      githubOwner := "rf972",
//      githubRepository := "s3datasource",
//      credentials +=
//        Credentials(
//          "GitHub Package Registry",
//          "maven.pkg.github.com",
//          sys.env.get("GITHUB_ACTOR").getOrElse("N/A"),
//          sys.env.getOrElse("GITHUB_TOKEN", "N/A")
//        ),
  //scala version, organization, version etc are also here
//)
