package org.apache.spark.deploy.history

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object EventLogFixtureBuilder {
  private val mapper = new ObjectMapper()

  final case class Fixture(eventLog: Path, appId: String, appAttemptId: Option[String])

  def generate(appAttemptId: Option[String] = None): Fixture = {
    val eventLogDir = Files.createTempDirectory("spark-eventlog-dir-")
    val warehouseDir = Files.createTempDirectory("spark-warehouse-dir-")

    val conf = new SparkConf()
      .setAppName(s"eventlog-preprocessor-suite-${appAttemptId.getOrElse("default")}")
      .setMaster("local[2]")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.compress", "false")
      .set("spark.eventLog.overwrite", "true")
      .set("spark.eventLog.dir", eventLogDir.toUri.toString)
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.sql.warehouse.dir", warehouseDir.toUri.toString)
      .set("spark.ui.enabled", "false")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.driver.host", "127.0.0.1")
    appAttemptId.foreach(conf.set("spark.app.attempt.id", _))

    val spark = SparkSession.builder().config(conf).getOrCreate()
    try {
      spark.sparkContext.setLogLevel("ERROR")

      val reduced = spark.sparkContext.parallelize(1 to 120, 4)
        .map(n => (n % 6, n * 2))
        .reduceByKey(_ + _)
        .collect()
      require(reduced.length == 6, s"Expected 6 reduce results, found ${reduced.length}")

      spark.range(0, 24).selectExpr("id", "id % 4 AS bucket")
        .createOrReplaceTempView("numbers")
      val sqlRows = spark.sql(
        """
          |SELECT bucket, COUNT(*) AS cnt, SUM(id) AS total
          |FROM numbers
          |GROUP BY bucket
          |ORDER BY bucket
          |""".stripMargin).collect()
      require(sqlRows.length == 4, s"Expected 4 SQL rows, found ${sqlRows.length}")

      val applicationId = spark.sparkContext.applicationId
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()

      val eventLog = listFiles(eventLogDir).find(path => !path.getFileName.toString.startsWith("."))
        .getOrElse(throw new IllegalStateException(s"No event log found in $eventLogDir"))
      appAttemptId.foreach(injectAppAttemptId(eventLog, _))
      val metadata = readApplicationStart(eventLog)

      require(metadata.appId == applicationId,
        s"Expected app id $applicationId in event log, found ${metadata.appId}")
      require(appAttemptId.forall(_ == metadata.appAttemptId.orNull),
        s"Expected attempt ${appAttemptId.orNull}, found ${metadata.appAttemptId.orNull}")

      Fixture(eventLog = eventLog, appId = metadata.appId, appAttemptId = metadata.appAttemptId)
    } finally {
      if (!spark.sparkContext.isStopped) {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  private def listFiles(path: Path): Seq[Path] = {
    val stream = Files.list(path)
    try {
      stream.iterator().asScala.toSeq
    } finally {
      stream.close()
    }
  }

  private def readApplicationStart(eventLog: Path): AppStartMetadata = {
    val line = Files.readAllLines(eventLog).asScala.find(_.contains("\"Event\":\"SparkListenerApplicationStart\""))
      .getOrElse(throw new IllegalStateException(s"No application-start event found in $eventLog"))
    val node = mapper.readTree(line)
    AppStartMetadata(
      appId = node.get("App ID").asText(),
      appAttemptId = Option(node.get("App Attempt ID")).filterNot(_.isNull).map(_.asText()))
  }

  private def injectAppAttemptId(eventLog: Path, attemptId: String): Unit = {
    val updatedLines = Files.readAllLines(eventLog).asScala.map { line =>
      if (line.contains("\"Event\":\"SparkListenerApplicationStart\"")) {
        val node = mapper.readTree(line).deepCopy[ObjectNode]()
        node.put("App Attempt ID", attemptId)
        mapper.writeValueAsString(node)
      } else {
        line
      }
    }
    Files.write(eventLog, updatedLines.asJava, StandardCharsets.UTF_8)
  }

  private case class AppStartMetadata(appId: String, appAttemptId: Option[String])
}
