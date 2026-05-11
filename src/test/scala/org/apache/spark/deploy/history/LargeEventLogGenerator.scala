package org.apache.spark.deploy.history

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
 * Generates a large number of Spark jobs / SQL queries to produce
 * both a traditional event log and a UIMeta snapshot for benchmarking.
 *
 * Supports both fixed-job-count mode and target-event-log-size mode.
 *
 * Examples:
 *   spark-submit --class org.apache.spark.deploy.history.LargeEventLogGenerator \
 *     spark-uiservice-tests.jar 100 1000
 *
 *   spark-submit --class org.apache.spark.deploy.history.LargeEventLogGenerator \
 *     spark-uiservice-tests.jar \
 *     --target-eventlog-gb 4 \
 *     --tasks-per-job 4096 \
 *     --records-per-task 1 \
 *     --sql-every-jobs 0 \
 *     --sql-queries-per-batch 1
 */
object LargeEventLogGenerator {

  private case class GeneratorConfig(
      numJobs: Int = 100,
      tasksPerJob: Int = 1000,
      recordsPerTask: Long = 100L,
      targetEventLogBytes: Option[Long] = None,
      maxJobs: Int = 100000,
      progressEveryJobs: Int = 10,
      sqlQueriesPerBatch: Int = 20,
      sqlRowsPerQuery: Long = 100000L,
      sqlEveryJobs: Int = 0)

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    val conf = new SparkConf()
      .setAppName("LargeEventLogGenerator")
      .setMaster("local[*]")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "file:///tmp/spark-events")
      .set("spark.extraListeners",
        "org.apache.spark.deploy.history.UIMetaLoggingListener")
      .set("spark.uimeta.dir", "file:///tmp/spark-uimeta")

    new File("/tmp/spark-events").mkdirs()
    new File("/tmp/spark-uimeta").mkdirs()

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    val appId = sc.applicationId

    // Keep benchmark output readable by muting per-task Spark INFO logs.
    sc.setLogLevel("WARN")

    val generationMode = config.targetEventLogBytes match {
      case Some(bytes) =>
        f"target event log size ${bytes.toDouble / 1024 / 1024 / 1024}%.2f GB"
      case None =>
        s"${config.numJobs} jobs"
    }

    println(s"=== Generating benchmark data for app $appId ===")
    println(s"Mode               : $generationMode")
    println(s"Tasks per job      : ${config.tasksPerJob}")
    println(s"Records per task   : ${config.recordsPerTask}")
    println(s"SQL queries/batch  : ${config.sqlQueriesPerBatch}")
    println(s"SQL every N jobs   : ${config.sqlEveryJobs}")

    var jobsRun = 0
    var sqlBatchIndex = 0

    while (shouldContinue(config, jobsRun, appId)) {
      runWideJob(sc, config)
      jobsRun += 1

      if (config.sqlEveryJobs > 0 && jobsRun % config.sqlEveryJobs == 0) {
        runSqlBatch(spark, config, sqlBatchIndex)
        sqlBatchIndex += config.sqlQueriesPerBatch
      }

      if (jobsRun % config.progressEveryJobs == 0 || jobsRun == 1) {
        printProgress(config, jobsRun, appId)
      }
    }

    if (config.sqlEveryJobs == 0 || sqlBatchIndex == 0) {
      runSqlBatch(spark, config, sqlBatchIndex)
      sqlBatchIndex += config.sqlQueriesPerBatch
    }

    printProgress(config, jobsRun, appId)
    spark.stop()
    println("=== Generation complete. Check /tmp/spark-events & /tmp/spark-uimeta ===")
  }

  private def parseArgs(args: Array[String]): GeneratorConfig = {
    var config = GeneratorConfig()
    val positionals = ArrayBuffer.empty[String]

    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--target-eventlog-gb" =>
          i += 1
          config = config.copy(targetEventLogBytes = Some(parseGb(args(i))))
        case "--target-eventlog-mb" =>
          i += 1
          config = config.copy(targetEventLogBytes = Some(parseMb(args(i))))
        case "--tasks-per-job" =>
          i += 1
          config = config.copy(tasksPerJob = args(i).toInt)
        case "--records-per-task" =>
          i += 1
          config = config.copy(recordsPerTask = args(i).toLong)
        case "--progress-every-jobs" =>
          i += 1
          config = config.copy(progressEveryJobs = args(i).toInt)
        case "--sql-queries-per-batch" =>
          i += 1
          config = config.copy(sqlQueriesPerBatch = args(i).toInt)
        case "--sql-rows-per-query" =>
          i += 1
          config = config.copy(sqlRowsPerQuery = args(i).toLong)
        case "--sql-every-jobs" =>
          i += 1
          config = config.copy(sqlEveryJobs = args(i).toInt)
        case "--max-jobs" =>
          i += 1
          config = config.copy(maxJobs = args(i).toInt)
        case value if value.startsWith("--") =>
          throw new IllegalArgumentException(s"Unknown option: $value")
        case value =>
          positionals += value
      }
      i += 1
    }

    if (positionals.nonEmpty) {
      config = config.copy(numJobs = positionals(0).toInt)
    }
    if (positionals.length >= 2) {
      config = config.copy(tasksPerJob = positionals(1).toInt)
    }
    if (positionals.length >= 3) {
      config = config.copy(recordsPerTask = positionals(2).toLong)
    }

    require(config.tasksPerJob > 0, "tasks-per-job must be > 0")
    require(config.recordsPerTask > 0, "records-per-task must be > 0")
    require(config.progressEveryJobs > 0, "progress-every-jobs must be > 0")
    require(config.sqlQueriesPerBatch >= 0, "sql-queries-per-batch must be >= 0")
    require(config.sqlRowsPerQuery > 0, "sql-rows-per-query must be > 0")
    require(config.sqlEveryJobs >= 0, "sql-every-jobs must be >= 0")
    require(config.maxJobs > 0, "max-jobs must be > 0")

    config
  }

  private def parseGb(value: String): Long = {
    (value.toDouble * 1024 * 1024 * 1024).toLong
  }

  private def parseMb(value: String): Long = {
    (value.toDouble * 1024 * 1024).toLong
  }

  private def shouldContinue(config: GeneratorConfig, jobsRun: Int, appId: String): Boolean = {
    val underJobLimit = jobsRun < (if (config.targetEventLogBytes.isDefined) config.maxJobs else config.numJobs)
    val underTarget = config.targetEventLogBytes.forall { target =>
      currentEventLogSizeBytes(appId) < target
    }
    underJobLimit && underTarget
  }

  private def runWideJob(sc: org.apache.spark.SparkContext, config: GeneratorConfig): Unit = {
    val totalRecords = config.tasksPerJob.toLong * config.recordsPerTask
    sc.range(0L, totalRecords, 1L, config.tasksPerJob)
      .map { id =>
        val key = (id % 2048).toInt
        val value = ((id * 31) ^ (id >>> 3)) % 100000
        (key, value)
      }
      .filter { case (_, value) => value % 3 != 0 }
      .reduceByKey(_ + _)
      .sortByKey()
      .count()
  }

  private def runSqlBatch(
      spark: SparkSession,
      config: GeneratorConfig,
      batchStartIndex: Int): Unit = {
    if (config.sqlQueriesPerBatch == 0) {
      return
    }

    val partitions = math.max(8, math.min(config.tasksPerJob, 2048))
    for (offset <- 0 until config.sqlQueriesPerBatch) {
      val queryId = batchStartIndex + offset + 1
      val viewName = s"t_$queryId"
      spark.range(0L, config.sqlRowsPerQuery, 1L, partitions)
        .selectExpr("id", "id % 1024 AS bucket", "(id * 17) % 10000 AS metric")
        .createOrReplaceTempView(viewName)

      spark.sql(
        s"""
           |SELECT bucket, COUNT(*) AS cnt, SUM(metric) AS total_metric
           |FROM $viewName
           |GROUP BY bucket
           |ORDER BY total_metric DESC
           |LIMIT 20
           |""".stripMargin).collect()
    }
  }

  private def printProgress(config: GeneratorConfig, jobsRun: Int, appId: String): Unit = {
    val eventBytes = currentEventLogSizeBytes(appId)
    val metaBytes = currentUIMetaSizeBytes(appId)
    val eventGb = eventBytes.toDouble / 1024 / 1024 / 1024
    val metaMb = metaBytes.toDouble / 1024 / 1024
    val progress = config.targetEventLogBytes match {
      case Some(target) =>
        f" (${eventBytes.toDouble / target * 100}%.1f%% of target)"
      case None =>
        ""
    }

    println(
      f"  Progress: jobs=$jobsRun eventLog=${eventGb}%.2f GB uimeta=${metaMb}%.2f MB$progress")
  }

  private def currentEventLogSizeBytes(appId: String): Long = {
    fileSizeWithFallback("/tmp/spark-events", appId)
  }

  private def currentUIMetaSizeBytes(appId: String): Long = {
    val directMatch = new File("/tmp/spark-uimeta", s"${appId}_1.uimeta")
    val inProgress = new File("/tmp/spark-uimeta", s"${appId}_1.uimeta.inprogress")
    if (directMatch.exists()) directMatch.length()
    else if (inProgress.exists()) inProgress.length()
    else 0L
  }

  private def fileSizeWithFallback(dir: String, baseName: String): Long = {
    val finalFile = new File(dir, baseName)
    val inProgressFile = new File(dir, s"$baseName.inprogress")
    if (finalFile.exists()) finalFile.length()
    else if (inProgressFile.exists()) inProgressFile.length()
    else 0L
  }
}
