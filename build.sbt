name := "HelloWorld"

version := "1.0"

scalaVersion := "2.12.1"

mainClass in(Compile, run) := Some("com.opencsi.Hello")

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.1",
  "org.apache.hadoop" % "hadoop-client" % "2.7.0"
)
