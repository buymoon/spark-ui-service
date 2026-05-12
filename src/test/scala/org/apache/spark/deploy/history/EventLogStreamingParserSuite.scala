package org.apache.spark.deploy.history

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.NoSuchElementException

import org.apache.hadoop.conf.Configuration
import org.apache.spark.status.{
  ExecutorStageSummaryWrapper,
  JobDataWrapper,
  RDDOperationGraphWrapper,
  StageDataWrapper
}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.ui.{SQLExecutionUIData, SparkPlanGraphWrapper}
import org.apache.spark.util.kvstore.InMemoryStore
import org.scalatest.funsuite.AnyFunSuite

class EventLogStreamingParserSuite extends AnyFunSuite {

  test("stream-v2 preserves more than 1000 stages") {
    val tempDir = Files.createTempDirectory("streaming-parser-suite")
    val eventLog = tempDir.resolve("local-stream-v2")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    writeEventLog(
      path = eventLog,
      appId = "local-stream-v2",
      appName = "Streaming Parser App",
      attemptId = Some("attempt-1205"),
      numStages = 1205)

    val hadoopConf = new Configuration()
    val result = EventLogStreamingParser.writeUIMeta(
      eventLog = eventLog.toUri.toString,
      uimetaDir = uimetaDir.toUri.toString,
      attemptIdOverride = None,
      compression = "none",
      taskShardRecords = 100000,
      hadoopConf = hadoopConf)

    assert(result.appId == "local-stream-v2")
    assert(result.attemptId.contains("attempt-1205"))
    assert(result.mode == "stream-v2")

    val manifest = UIMetaV2Manifest.read(result.uimetaPath, hadoopConf)
    assert(manifest.completed)
    assert(manifest.counts.jobs == 1205L)
    assert(manifest.counts.stages == 1205L)

    val store = new InMemoryStore()
    val reader = new UIMetaV2Reader(uimetaDir.toUri.toString, hadoopConf)
    reader.loadShards(manifest, manifest.summaryShards, store)

    assert(count(store.view(classOf[JobDataWrapper]).iterator()) == 1205)
    assert(count(store.view(classOf[StageDataWrapper]).iterator()) == 1205)
  }

  test("stream-v2 splits summary shards into at most 5000 records") {
    val tempDir = Files.createTempDirectory("stream-v2-summary-shards")
    val eventLog = tempDir.resolve("local-stream-v2-summary-shards")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    writeEventLog(
      path = eventLog,
      appId = "local-stream-v2-summary-shards",
      appName = "Streaming Parser Summary Shards",
      attemptId = None,
      numStages = 5001)

    val hadoopConf = new Configuration()
    val result = EventLogStreamingParser.writeUIMeta(
      eventLog = eventLog.toUri.toString,
      uimetaDir = uimetaDir.toUri.toString,
      attemptIdOverride = None,
      compression = "none",
      taskShardRecords = 100000,
      hadoopConf = hadoopConf)

    val manifest = UIMetaV2Manifest.read(result.uimetaPath, hadoopConf)
    val jobShardCounts = manifest.shardsForKind("jobs").map(_.recordCount)
    val stageShardCounts = manifest.shardsForKind("stages").map(_.recordCount)
    assert(jobShardCounts == Seq(5000L, 1L))
    assert(stageShardCounts == Seq(5000L, 5000L, 2L))
    assert(manifest.summaryShards.forall(_.recordCount <= 5000L))
  }

  test("stream-v2 tolerates malformed final line for in-progress logs") {
    val tempDir = Files.createTempDirectory("streaming-parser-inprogress-suite")
    val eventLog = tempDir.resolve("local-stream-v2-inprogress.inprogress")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val lines = Seq(
      applicationStart("local-stream-v2-inprogress", "Streaming Parser In Progress", None),
      environmentUpdate("local-stream-v2-inprogress", "Streaming Parser In Progress", None),
      stageAndJobEvents(1).head,
      """{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":1,"Stage Attempt ID":0,"Stage Name":"stage-1","Number of Tasks":1,"RDD Info":[],"Parent IDs":[],"Details":"stage-1 details","Submission Time":2010,"Accumulables":[],"Resource Profile Id":0},"Properties":{}}""",
      """{"Event":"SparkListenerStageCompleted","Stage Info":"""
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val hadoopConf = new Configuration()
    val result = EventLogStreamingParser.writeUIMeta(
      eventLog = eventLog.toUri.toString,
      uimetaDir = uimetaDir.toUri.toString,
      attemptIdOverride = None,
      compression = "none",
      taskShardRecords = 100000,
      hadoopConf = hadoopConf)

    val manifest = UIMetaV2Manifest.read(result.uimetaPath, hadoopConf)
    assert(!manifest.completed)
    assert(manifest.counts.jobs == 1L)
    assert(manifest.counts.stages == 1L)

    val store = new InMemoryStore()
    val reader = new UIMetaV2Reader(uimetaDir.toUri.toString, hadoopConf)
    reader.loadShards(manifest, manifest.summaryShards, store)
    assert(count(store.view(classOf[StageDataWrapper]).iterator()) == 1)
  }

  test("stream-v2 writes task records into per-stage shards") {
    val tempDir = Files.createTempDirectory("stream-v2-task-shards")
    val eventLog = tempDir.resolve("local-task-shards")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val lines = Seq(
      """{"Event":"SparkListenerApplicationStart","App Name":"Task Shards","App ID":"local-task-shards","Timestamp":1000,"User":"alice"}""",
      """{"Event":"SparkListenerJobStart","Job ID":1,"Submission Time":2000,"Stage Infos":[{"Stage ID":2,"Stage Attempt ID":0,"Stage Name":"tasks","Number of Tasks":3,"RDD Info":[],"Parent IDs":[],"Details":"tasks","Submission Time":2100,"Resource Profile Id":0}],"Stage IDs":[2],"Properties":{}}""",
      """{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":2,"Stage Attempt ID":0,"Stage Name":"tasks","Number of Tasks":3,"RDD Info":[],"Parent IDs":[],"Details":"tasks","Submission Time":2100,"Accumulables":[],"Resource Profile Id":0},"Properties":{}}""",
      taskEndJson(stageId = 2, attemptId = 0, taskId = 10L, index = 0, launchTime = 2200L, finishTime = 2300L),
      taskEndJson(stageId = 2, attemptId = 0, taskId = 11L, index = 1, launchTime = 2201L, finishTime = 2301L),
      taskEndJson(stageId = 2, attemptId = 0, taskId = 12L, index = 2, launchTime = 2202L, finishTime = 2302L),
      """{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":2,"Stage Attempt ID":0,"Stage Name":"tasks","Number of Tasks":3,"RDD Info":[],"Parent IDs":[],"Details":"tasks","Submission Time":2100,"Completion Time":2400,"Accumulables":[],"Resource Profile Id":0}}""",
      """{"Event":"SparkListenerJobEnd","Job ID":1,"Completion Time":2500,"Job Result":{"Result":"JobSucceeded"}}""",
      """{"Event":"SparkListenerApplicationEnd","Timestamp":2600}"""
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val hadoopConf = new Configuration()
    val result = EventLogStreamingParser.writeUIMeta(
      eventLog.toUri.toString,
      uimetaDir.toUri.toString,
      None,
      compression = "none",
      taskShardRecords = 2,
      hadoopConf = hadoopConf)

    val manifest = UIMetaV2Manifest.read(result.uimetaPath, hadoopConf)
    assert(manifest.counts.tasks == 3)
    assert(manifest.taskShardsFor(2, 0).map(_.recordCount).sum == 3)
    assert(manifest.taskShardsFor(2, 0).size == 2)

    val store = new InMemoryStore()
    val reader = new UIMetaV2Reader(uimetaDir.toUri.toString, hadoopConf)
    reader.loadShards(manifest, manifest.taskShardsFor(2, 0), store)
    assert(count(store.view(classOf[org.apache.spark.status.TaskDataWrapper]).iterator()) == 3)
    val task = store.read(classOf[org.apache.spark.status.TaskDataWrapper], 10L)
    assert(task.shuffleRecordsRead == 10L)
  }

  test("stream-v2 uses authoritative app id for task shard paths") {
    val tempDir = Files.createTempDirectory("stream-v2-task-authoritative-app-id")
    val eventLog = tempDir.resolve("path-derived-app-id")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val lines = Seq(
      """{"Event":"SparkListenerApplicationStart","App Name":"Task Shards","Timestamp":1000,"User":"alice"}""",
      environmentUpdate("authoritative-app-id", "Task Shards", Some("attempt-1")),
      """{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":2,"Stage Attempt ID":0,"Stage Name":"tasks","Number of Tasks":1,"RDD Info":[],"Parent IDs":[],"Details":"tasks","Submission Time":2100,"Accumulables":[],"Resource Profile Id":0},"Properties":{}}""",
      taskEndJson(stageId = 2, attemptId = 0, taskId = 10L, index = 0, launchTime = 2200L, finishTime = 2300L),
      """{"Event":"SparkListenerApplicationEnd","Timestamp":2600}"""
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val hadoopConf = new Configuration()
    val result = EventLogStreamingParser.writeUIMeta(
      eventLog.toUri.toString,
      uimetaDir.toUri.toString,
      None,
      compression = "none",
      taskShardRecords = 1,
      hadoopConf = hadoopConf)

    val manifest = UIMetaV2Manifest.read(result.uimetaPath, hadoopConf)
    assert(result.appId == "authoritative-app-id")
    assert(result.attemptId.contains("attempt-1"))
    assert(manifest.appId == "authoritative-app-id")
    assert(manifest.attemptId.contains("attempt-1"))
    assert(manifest.taskShardsFor(2, 0).head.path.startsWith("authoritative-app-id_attempt-1/"))
  }

  test("stream-v2 uses Spark task defaults and preserves user accumulators") {
    val tempDir = Files.createTempDirectory("stream-v2-task-defaults")
    val eventLog = tempDir.resolve("local-task-defaults")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val lines = Seq(
      """{"Event":"SparkListenerApplicationStart","App Name":"Task Defaults","App ID":"local-task-defaults","Timestamp":1000,"User":"alice"}""",
      """{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":2,"Stage Attempt ID":0,"Stage Name":"tasks","Number of Tasks":1,"RDD Info":[],"Parent IDs":[],"Details":"tasks","Submission Time":2100,"Accumulables":[],"Resource Profile Id":0},"Properties":{}}""",
      minimalTaskEndJson(stageId = 2, attemptId = 0, taskId = 20L),
      """{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":2,"Stage Attempt ID":0,"Stage Name":"tasks","Number of Tasks":1,"RDD Info":[],"Parent IDs":[],"Details":"tasks","Submission Time":2100,"Completion Time":2400,"Accumulables":[],"Resource Profile Id":0}}""",
      """{"Event":"SparkListenerApplicationEnd","Timestamp":2600}"""
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val hadoopConf = new Configuration()
    val result = EventLogStreamingParser.writeUIMeta(
      eventLog.toUri.toString,
      uimetaDir.toUri.toString,
      None,
      compression = "none",
      taskShardRecords = 100000,
      hadoopConf = hadoopConf)

    val manifest = UIMetaV2Manifest.read(result.uimetaPath, hadoopConf)
    val store = new InMemoryStore()
    val reader = new UIMetaV2Reader(uimetaDir.toUri.toString, hadoopConf)
    reader.loadShards(manifest, manifest.taskShardsFor(2, 0), store)

    val task = store.read(classOf[org.apache.spark.status.TaskDataWrapper], 20L)
    assert(task.attempt == 1)
    assert(task.partitionId == -1)
    assert(task.hasMetrics)
    assert(task.executorCpuTime == 0L)
    assert(task.shuffleRecordsRead == 0L)
    assert(task.accumulatorUpdates.map(_.name) == Seq("user-acc"))
    assert(task.accumulatorUpdates.head.value == "42")
  }

  test("stream-v2 preserves stage graph, accumulators, and task aggregates") {
    val tempDir = Files.createTempDirectory("stream-v2-stage-rich-data")
    val eventLog = tempDir.resolve("local-stage-rich-data")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val stageInfo =
      """{"Stage ID":2,"Stage Attempt ID":0,"Stage Name":"rich-stage","Number of Tasks":2,"RDD Info":[{"RDD ID":10,"Name":"ParallelCollectionRDD","Scope":"{\"id\":\"scope-outer\",\"name\":\"parallelize\"}","Callsite":"callsite-a","Parent IDs":[],"Storage Level":{"Use Disk":false,"Use Memory":true,"Deserialized":false,"Replication":1},"Barrier":false,"DeterministicLevel":"DETERMINATE"},{"RDD ID":11,"Name":"MapPartitionsRDD","Scope":"{\"id\":\"scope-inner\",\"name\":\"map\",\"parent\":{\"id\":\"scope-outer\",\"name\":\"parallelize\"}}","Callsite":"callsite-b","Parent IDs":[10],"Storage Level":{"Use Disk":false,"Use Memory":false,"Deserialized":false,"Replication":1},"Barrier":false,"DeterministicLevel":"UNORDERED"}],"Parent IDs":[],"Details":"rich-stage details","Submission Time":2100,"Accumulables":[{"ID":50,"Name":"stage-user-acc","Update":"2","Value":"2","Internal":false,"Metadata":""},{"ID":51,"Name":"internal.metrics.executorRunTime","Update":20,"Value":20,"Internal":true,"Metadata":""},{"ID":52,"Name":"sql-acc","Update":"3","Value":"3","Internal":false,"Metadata":"sql"}],"Resource Profile Id":0}"""
    val lines = Seq(
      applicationStart("local-stage-rich-data", "Stage Rich Data", None),
      s"""{"Event":"SparkListenerJobStart","Job ID":1,"Submission Time":2000,"Stage Infos":[$stageInfo],"Stage IDs":[2],"Properties":{"spark.job.description":"job-rich"}}""",
      s"""{"Event":"SparkListenerStageSubmitted","Stage Info":$stageInfo,"Properties":{"spark.job.description":"stage-rich-description","spark.scheduler.pool":"pool-a"}}""",
      richTaskEndJson(
        taskId = 20L,
        index = 0,
        attempt = 0,
        executorId = "1",
        locality = "NODE_LOCAL",
        launchTime = 2200L,
        finishTime = 2230L,
        status = "failed"),
      richTaskEndJson(
        taskId = 21L,
        index = 0,
        attempt = 1,
        executorId = "1",
        locality = "NODE_LOCAL",
        launchTime = 2240L,
        finishTime = 2300L,
        status = "success"),
      richTaskEndJson(
        taskId = 22L,
        index = 1,
        attempt = 0,
        executorId = "2",
        locality = "RACK_LOCAL",
        launchTime = 2250L,
        finishTime = 2320L,
        status = "success"),
      s"""{"Event":"SparkListenerStageCompleted","Stage Info":${stageInfo.dropRight(1)},"Completion Time":2400}}""",
      """{"Event":"SparkListenerJobEnd","Job ID":1,"Completion Time":2500,"Job Result":{"Result":"JobSucceeded"}}""",
      applicationEnd(2600L)
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val hadoopConf = new Configuration()
    val result = EventLogStreamingParser.writeUIMeta(
      eventLog.toUri.toString,
      uimetaDir.toUri.toString,
      None,
      compression = "none",
      taskShardRecords = 100000,
      hadoopConf = hadoopConf)

    val manifest = UIMetaV2Manifest.read(result.uimetaPath, hadoopConf)
    val store = new InMemoryStore()
    val reader = new UIMetaV2Reader(uimetaDir.toUri.toString, hadoopConf)
    reader.loadShards(manifest, manifest.summaryShards, store)

    val stage = store.read(classOf[StageDataWrapper], Array(2, 0))
    assert(stage.info.description.contains("stage-rich-description"))
    assert(stage.info.schedulingPool == "pool-a")
    assert(stage.info.accumulatorUpdates.map(_.name) == Seq("stage-user-acc"))
    assert(stage.info.firstTaskLaunchedTime.map(_.getTime).contains(2200L))
    assert(stage.info.numCompleteTasks == 2)
    assert(stage.info.numFailedTasks == 1)
    assert(stage.info.numCompletedIndices == 2)
    assert(stage.locality == Map("NODE_LOCAL" -> 2L, "RACK_LOCAL" -> 1L))

    val job = store.read(classOf[JobDataWrapper], 1)
    assert(job.info.numCompletedTasks == 2)
    assert(job.info.numFailedTasks == 1)
    assert(job.info.numCompletedIndices == 2)

    val graph = store.read(classOf[RDDOperationGraphWrapper], 2).toRDDOperationGraph()
    assert(graph.edges.map(edge => edge.fromId -> edge.toId) == Seq(10 -> 11))
    assert(graph.rootCluster.childClusters.map(_.name) == Seq("parallelize"))
    assert(graph.rootCluster.childClusters.head.childClusters.map(_.name) == Seq("map"))
    assert(graph.rootCluster.childClusters.head.childClusters.head.childNodes.map(_.name) ==
      Seq("MapPartitionsRDD"))

    val executor1 = store.read(classOf[ExecutorStageSummaryWrapper], Array(2, 0, "1"))
    assert(executor1.info.failedTasks == 1)
    assert(executor1.info.succeededTasks == 1)
    assert(executor1.info.taskTime == 90L)

    reader.loadShards(manifest, manifest.taskShardsFor(2, 0), store)
    assert(count(store.view(classOf[org.apache.spark.status.TaskDataWrapper]).iterator()) == 3)
  }

  test("stream-v2 writes SQL execution records when SQL events exist") {
    val tempDir = Files.createTempDirectory("stream-v2-sql")
    val eventLog = tempDir.resolve("local-sql")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val lines = Seq(
      """{"Event":"SparkListenerApplicationStart","App Name":"SQL App","App ID":"local-sql","Timestamp":1000,"User":"alice"}""",
      sqlExecutionStartJson(99L),
      """{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd","executionId":99,"time":1200}""",
      """{"Event":"SparkListenerApplicationEnd","Timestamp":1300}"""
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val hadoopConf = new Configuration()
    val result = EventLogStreamingParser.writeUIMeta(
      eventLog.toUri.toString,
      uimetaDir.toUri.toString,
      None,
      compression = "none",
      taskShardRecords = 100000,
      hadoopConf = hadoopConf)

    val manifest = UIMetaV2Manifest.read(result.uimetaPath, hadoopConf)
    assert(manifest.counts.sqlExecutions == 1L)
    assert(manifest.summaryShards.exists(_.kind == "sql"))

    val store = new InMemoryStore()
    val reader = new UIMetaV2Reader(uimetaDir.toUri.toString, hadoopConf)
    reader.loadShards(manifest, manifest.summaryShards, store)
    assert(count(store.view(classOf[SQLExecutionUIData]).iterator()) == 1)
    assert(count(store.view(classOf[SparkPlanGraphWrapper]).iterator()) == 1)
    val execution = store.read(classOf[SQLExecutionUIData], 99L)
    assert(execution.description == "select 1")
    assert(execution.completionTime.isDefined)
  }

  test("stream-v2 preserves SQL DAG metrics and query stage links") {
    val tempDir = Files.createTempDirectory("stream-v2-sql-metrics")
    val eventLog = tempDir.resolve("local-sql-metrics")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val lines = Seq(
      applicationStart("local-sql-metrics", "SQL Metrics", None),
      sqlExecutionStartJsonWithMetrics(99L),
      s"""{"Event":"SparkListenerJobStart","Job ID":0,"Submission Time":2000,"Stage Infos":[{"Stage ID":0,"Stage Attempt ID":0,"Stage Name":"sql-stage","Number of Tasks":2,"RDD Info":[{"RDD ID":0}],"Parent IDs":[],"Details":"sql-stage details","Submission Time":2000,"Resource Profile Id":0}],"Stage IDs":[0],"Properties":{"spark.job.description":"sql-job","spark.sql.execution.id":"99"}}""",
      s"""{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":0,"Stage Attempt ID":0,"Stage Name":"sql-stage","Number of Tasks":2,"RDD Info":[{"RDD ID":0}],"Parent IDs":[],"Details":"sql-stage details","Submission Time":2000,"Accumulables":[],"Resource Profile Id":0},"Properties":{}}""",
      sqlMetricTaskEndJson(taskId = 0L, index = 0, outputRows = 3L, scanTime = 100L),
      sqlMetricTaskEndJson(taskId = 1L, index = 1, outputRows = 4L, scanTime = 200L),
      s"""{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":0,"Stage Attempt ID":0,"Stage Name":"sql-stage","Number of Tasks":2,"RDD Info":[{"RDD ID":0}],"Parent IDs":[],"Details":"sql-stage details","Submission Time":2000,"Completion Time":2300,"Accumulables":[],"Resource Profile Id":0}}""",
      """{"Event":"SparkListenerJobEnd","Job ID":0,"Completion Time":2310,"Job Result":{"Result":"JobSucceeded"}}""",
      """{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd","executionId":99,"time":2400}""",
      applicationEnd(2500L)
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val hadoopConf = new Configuration()
    val result = EventLogStreamingParser.writeUIMeta(
      eventLog.toUri.toString,
      uimetaDir.toUri.toString,
      None,
      compression = "none",
      taskShardRecords = 100000,
      hadoopConf = hadoopConf)

    val manifest = UIMetaV2Manifest.read(result.uimetaPath, hadoopConf)
    val store = new InMemoryStore()
    val reader = new UIMetaV2Reader(uimetaDir.toUri.toString, hadoopConf)
    reader.loadShards(manifest, manifest.summaryShards, store)

    val execution = store.read(classOf[SQLExecutionUIData], 99L)
    assert(execution.jobs == Map(0 -> org.apache.spark.JobExecutionStatus.SUCCEEDED))
    assert(execution.stages == Set(0))
    assert(execution.metricValues(7L) == "7")
    assert(execution.metricValues(8L) ==
      SQLMetrics.stringValue("timing", Array(100L, 200L), Array(200L, 0L, 0L, 1L)))

    val graph = store.read(classOf[SparkPlanGraphWrapper], 99L).toSparkPlanGraph()
    assert(graph.nodes.size == 1)
    assert(graph.allNodes.flatMap(_.metrics).map(_.accumulatorId).toSet == Set(7L, 8L))
    assert(graph.makeDotFile(execution.metricValues).contains("number of output rows: 7"))
    assert(graph.makeDotFile(execution.metricValues).contains("stage 0.0: task 1"))
  }

  test("stream-v2 bounds SQL executions from retained executions config") {
    val tempDir = Files.createTempDirectory("stream-v2-sql-retention")
    val eventLog = tempDir.resolve("local-sql-retention")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val lines = Seq(
      applicationStart("local-sql-retention", "SQL Retention", None),
      environmentUpdate(
        "local-sql-retention",
        "SQL Retention",
        None,
        Seq("spark.sql.ui.retainedExecutions" -> "2")),
      sqlExecutionStartJson(1L),
      """{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd","executionId":1,"time":1200}""",
      sqlExecutionStartJson(2L),
      """{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd","executionId":2,"time":1300}""",
      sqlExecutionStartJson(3L),
      """{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd","executionId":3,"time":1400}""",
      applicationEnd(1500L)
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val hadoopConf = new Configuration()
    val result = EventLogStreamingParser.writeUIMeta(
      eventLog.toUri.toString,
      uimetaDir.toUri.toString,
      None,
      compression = "none",
      taskShardRecords = 100000,
      hadoopConf = hadoopConf)

    val manifest = UIMetaV2Manifest.read(result.uimetaPath, hadoopConf)
    assert(manifest.counts.sqlExecutions == 2L)

    val store = new InMemoryStore()
    val reader = new UIMetaV2Reader(uimetaDir.toUri.toString, hadoopConf)
    reader.loadShards(manifest, manifest.summaryShards, store)
    assert(count(store.view(classOf[SQLExecutionUIData]).iterator()) == 2)
    assert(count(store.view(classOf[SparkPlanGraphWrapper]).iterator()) == 2)
    intercept[NoSuchElementException] {
      store.read(classOf[SQLExecutionUIData], 1L)
    }
    assert(store.read(classOf[SQLExecutionUIData], 2L).description == "select 1")
    assert(store.read(classOf[SQLExecutionUIData], 3L).description == "select 1")
  }

  private def writeEventLog(
      path: java.nio.file.Path,
      appId: String,
      appName: String,
      attemptId: Option[String],
      numStages: Int): Unit = {
    val lines =
      applicationStart(appId, appName, attemptId) +:
        environmentUpdate(appId, appName, attemptId) +:
        (0 until numStages).flatMap(stageAndJobEvents) :+
        applicationEnd(5000L + numStages * 10L)
    Files.write(path, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
  }

  private def applicationStart(appId: String, appName: String, attemptId: Option[String]): String = {
    val attempt = attemptId.map(id => s""","App Attempt ID":"$id"""").getOrElse("")
    s"""{"Event":"SparkListenerApplicationStart","App Name":"$appName","App ID":"$appId"$attempt,"Timestamp":1000,"User":"alice"}"""
  }

  private def environmentUpdate(
      appId: String,
      appName: String,
      attemptId: Option[String],
      extraSparkProperties: Seq[(String, String)] = Seq.empty): String = {
    val sparkProperties = Seq(
      "spark.app.id" -> appId,
      "spark.app.name" -> appName,
      "spark.executor.memory" -> "4g") ++
      attemptId.map(id => Seq("spark.app.attempt.id" -> id)).getOrElse(Seq.empty) ++
      extraSparkProperties
    val sparkPropertiesJson = sparkProperties.map { case (key, value) =>
      s""""$key":"$value""""
    }.mkString(",")
    s"""{"Event":"SparkListenerEnvironmentUpdate","JVM Information":{"Java Version":"11.0.20","Java Home":"/java","Scala Version":"version 2.12.15"},"Spark Properties":{$sparkPropertiesJson},"Hadoop Properties":{},"System Properties":{},"Classpath Entries":{}}"""
  }

  private def applicationEnd(timestamp: Long): String = {
    s"""{"Event":"SparkListenerApplicationEnd","Timestamp":$timestamp}"""
  }

  private def stageAndJobEvents(id: Int): Seq[String] = {
    val baseTime = 2000L + id * 10L
    Seq(
      s"""{"Event":"SparkListenerJobStart","Job ID":$id,"Submission Time":$baseTime,"Stage Infos":[{"Stage ID":$id,"Stage Attempt ID":0,"Stage Name":"stage-$id","Number of Tasks":1,"RDD Info":[{"RDD ID":$id}],"Parent IDs":[],"Details":"stage-$id details","Submission Time":$baseTime,"Resource Profile Id":0}],"Stage IDs":[$id],"Properties":{"spark.job.description":"job-$id"}}}""",
      s"""{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":$id,"Stage Attempt ID":0,"Stage Name":"stage-$id","Number of Tasks":1,"RDD Info":[{"RDD ID":$id}],"Parent IDs":[],"Details":"stage-$id details","Submission Time":$baseTime,"Accumulables":[],"Resource Profile Id":0},"Properties":{}}""",
      s"""{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":$id,"Stage Attempt ID":0,"Stage Name":"stage-$id","Number of Tasks":1,"RDD Info":[{"RDD ID":$id}],"Parent IDs":[],"Details":"stage-$id details","Submission Time":$baseTime,"Completion Time":${baseTime + 5},"Accumulables":[],"Resource Profile Id":0}}""",
      s"""{"Event":"SparkListenerJobEnd","Job ID":$id,"Completion Time":${baseTime + 6},"Job Result":{"Result":"JobSucceeded"}}"""
    )
  }

  private def taskEndJson(
      stageId: Int,
      attemptId: Int,
      taskId: Long,
      index: Int,
      launchTime: Long,
      finishTime: Long): String = {
    s"""{"Event":"SparkListenerTaskEnd","Stage ID":$stageId,"Stage Attempt ID":$attemptId,"Task Type":"ResultTask","Task End Reason":{"Reason":"Success"},"Task Info":{"Task ID":$taskId,"Index":$index,"Attempt":0,"Partition ID":$index,"Launch Time":$launchTime,"Finish Time":$finishTime,"Executor ID":"1","Host":"localhost","Locality":"PROCESS_LOCAL","Speculative":false,"Getting Result Time":0,"Failed":false,"Killed":false,"Accumulables":[]},"Task Metrics":{"Executor Deserialize Time":1,"Executor Deserialize CPU Time":2,"Executor Run Time":10,"Executor CPU Time":20,"Result Size":30,"JVM GC Time":0,"Result Serialization Time":1,"Memory Bytes Spilled":0,"Disk Bytes Spilled":0,"Peak Execution Memory":0,"Input Metrics":{"Bytes Read":100,"Records Read":10},"Output Metrics":{"Bytes Written":0,"Records Written":0},"Shuffle Read Metrics":{"Remote Blocks Fetched":0,"Local Blocks Fetched":0,"Fetch Wait Time":0,"Remote Bytes Read":0,"Remote Bytes Read To Disk":0,"Local Bytes Read":0,"Total Records Read":10},"Shuffle Write Metrics":{"Shuffle Bytes Written":0,"Shuffle Write Time":0,"Shuffle Records Written":0}}}"""
  }

  private def minimalTaskEndJson(stageId: Int, attemptId: Int, taskId: Long): String = {
    s"""{"Event":"SparkListenerTaskEnd","Stage ID":$stageId,"Stage Attempt ID":$attemptId,"Task Type":"ResultTask","Task End Reason":{"Reason":"Success"},"Task Info":{"Task ID":$taskId,"Index":0,"Launch Time":2200,"Finish Time":2300,"Executor ID":"1","Host":"localhost","Locality":"PROCESS_LOCAL","Getting Result Time":0,"Failed":false,"Accumulables":[{"ID":1,"Name":"user-acc","Update":"1","Value":"42","Internal":false,"Metadata":""},{"ID":2,"Name":"internal.metrics.executorRunTime","Update":10,"Value":10,"Internal":true,"Metadata":""},{"ID":3,"Name":"sql-acc","Update":"3","Value":"3","Internal":false,"Metadata":"sql"}]},"Task Metrics":{"Executor Deserialize Time":1}}"""
  }

  private def richTaskEndJson(
      taskId: Long,
      index: Int,
      attempt: Int,
      executorId: String,
      locality: String,
      launchTime: Long,
      finishTime: Long,
      status: String): String = {
    val failed = status == "failed"
    val endReason = if (failed) {
      """{"Reason":"ExceptionFailure","Description":"boom"}"""
    } else {
      """{"Reason":"Success"}"""
    }
    s"""{"Event":"SparkListenerTaskEnd","Stage ID":2,"Stage Attempt ID":0,"Task Type":"ResultTask","Task End Reason":$endReason,"Task Info":{"Task ID":$taskId,"Index":$index,"Attempt":$attempt,"Partition ID":$index,"Launch Time":$launchTime,"Finish Time":$finishTime,"Executor ID":"$executorId","Host":"host-$executorId","Locality":"$locality","Speculative":false,"Getting Result Time":0,"Failed":$failed,"Killed":false,"Accumulables":[]},"Task Metrics":{"Executor Deserialize Time":1,"Executor Deserialize CPU Time":2,"Executor Run Time":${finishTime - launchTime},"Executor CPU Time":20,"Result Size":30,"JVM GC Time":0,"Result Serialization Time":1,"Memory Bytes Spilled":0,"Disk Bytes Spilled":0,"Peak Execution Memory":0,"Input Metrics":{"Bytes Read":100,"Records Read":10},"Output Metrics":{"Bytes Written":0,"Records Written":0},"Shuffle Read Metrics":{"Remote Blocks Fetched":0,"Local Blocks Fetched":0,"Fetch Wait Time":0,"Remote Bytes Read":0,"Remote Bytes Read To Disk":0,"Local Bytes Read":0,"Total Records Read":10},"Shuffle Write Metrics":{"Shuffle Bytes Written":0,"Shuffle Write Time":0,"Shuffle Records Written":0}}}"""
  }

  private def sqlExecutionStartJson(executionId: Long): String = {
    s"""{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart","executionId":$executionId,"description":"select 1","details":"select 1","physicalPlanDescription":"LocalTableScan","sparkPlanInfo":{"nodeName":"LocalTableScan","simpleString":"LocalTableScan","children":[],"metadata":{},"metrics":[]},"time":1100,"modifiedConfigs":{"spark.sql.shuffle.partitions":"1"}}"""
  }

  private def sqlExecutionStartJsonWithMetrics(executionId: Long): String = {
    s"""{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart","executionId":$executionId,"description":"select metrics","details":"select metrics","physicalPlanDescription":"Scan with metrics","sparkPlanInfo":{"nodeName":"LocalTableScan","simpleString":"LocalTableScan","children":[],"metadata":{},"metrics":[{"name":"number of output rows","accumulatorId":7,"metricType":"sum"},{"name":"scan time","accumulatorId":8,"metricType":"timing"}]},"time":1100,"modifiedConfigs":{"spark.sql.shuffle.partitions":"1"}}"""
  }

  private def sqlMetricTaskEndJson(
      taskId: Long,
      index: Int,
      outputRows: Long,
      scanTime: Long): String = {
    s"""{"Event":"SparkListenerTaskEnd","Stage ID":0,"Stage Attempt ID":0,"Task Type":"ResultTask","Task End Reason":{"Reason":"Success"},"Task Info":{"Task ID":$taskId,"Index":$index,"Attempt":0,"Partition ID":$index,"Launch Time":2200,"Finish Time":2300,"Executor ID":"1","Host":"localhost","Locality":"PROCESS_LOCAL","Speculative":false,"Getting Result Time":0,"Failed":false,"Killed":false,"Accumulables":[{"ID":7,"Name":"number of output rows","Update":"$outputRows","Value":"$outputRows","Internal":true,"Metadata":"sql"},{"ID":8,"Name":"scan time","Update":"$scanTime","Value":"$scanTime","Internal":true,"Metadata":"sql"}]},"Task Metrics":{"Executor Deserialize Time":1,"Executor Run Time":10}}"""
  }

  private def count[A](iterator: java.util.Iterator[A]): Int = {
    var total = 0
    while (iterator.hasNext) {
      iterator.next()
      total += 1
    }
    total
  }
}
