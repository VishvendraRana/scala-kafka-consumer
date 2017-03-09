name := "kafka-consumer"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka_2.10" % "0.10.2.0",
  "org.slf4j" % "slf4j-api" % "1.7.24"
)
    