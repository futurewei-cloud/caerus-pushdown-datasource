/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 name := "pushdown-datasource"

organization := ""
version := "0.1.0"
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
  "org.apache.hadoop" % "hadoop-client" % "3.2.2",
  "org.slf4j" % "slf4j-simple" % "1.7.21" % Test,
  "org.apache.logging.log4j" % "log4j-api" % "2.14.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.14.0",
)
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

// scalastyle >= 0.9.0
compileScalastyle := scalastyle.in(Compile).toTask("").value

(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

// Create a default Scala style task to run with tests
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

// scalastyle >= 0.9.0
testScalastyle := scalastyle.in(Test).toTask("").value

(test in Test) := ((test in Test) dependsOn testScalastyle).value