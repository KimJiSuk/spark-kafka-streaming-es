name := "spark-kafka-streaming"

version := "1.0.1"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  DefaultMavenRepository,
  "Confluent Platform" at "https://packages.confluent.io/maven/"
)

val excludeJacksonBinding = ExclusionRule(organization = "com.fasterxml.jackson.module")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4" % Provided,
  "org.apache.spark" %% "spark-streaming" % "2.4.4" % Provided,
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.apache.spark" %% "spark-avro" % "2.4.4",
//  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4",
  "org.apache.avro" % "avro" % "1.9.1",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.httpcomponents" % "httpcore" % "4.4.12",
  "org.apache.httpcomponents" % "httpclient" % "4.5.10",
  "io.confluent" % "kafka-avro-serializer" % "5.3.0",
  "io.confluent" % "kafka-schema-registry-client" % "5.3.0",
  "io.confluent" % "common-config" % "5.3.0",
  "io.confluent" % "common-utils" % "5.3.0",
  "io.confluent" % "kafka-schema-registry" % "5.3.0" excludeAll(excludeJacksonBinding)
)