package org.apache.spark.deploy.history

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.status.{JobDataWrapper, StageDataWrapper, TaskDataWrapper}
import org.apache.spark.sql.execution.ui.SQLExecutionUIData
import org.apache.spark.util.kvstore.KVStore
import org.scalatest.funsuite.AnyFunSuite

class UIMetaProviderV2Suite extends AnyFunSuite {

  test("provider reads v2 manifest and loads summary shards") {
    val tempDir = Files.createTempDirectory("uimeta-provider-v2")
    val eventLog = tempDir.resolve("local-provider-v2")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val lines = Seq(
      """{"Event":"SparkListenerApplicationStart","App Name":"Provider V2","App ID":"local-provider-v2","Timestamp":1000,"User":"alice"}""",
      """{"Event":"SparkListenerJobStart","Job ID":1,"Submission Time":2000,"Stage Infos":[{"Stage ID":1,"Stage Attempt ID":0,"Stage Name":"stage-1","Number of Tasks":1,"RDD Info":[],"Parent IDs":[],"Details":"stage-1 details","Submission Time":2000,"Resource Profile Id":0}],"Stage IDs":[1],"Properties":{"spark.job.description":"job-1"}}""",
      """{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":1,"Stage Attempt ID":0,"Stage Name":"stage-1","Number of Tasks":1,"RDD Info":[],"Parent IDs":[],"Details":"stage-1 details","Submission Time":2000,"Accumulables":[],"Resource Profile Id":0},"Properties":{}}""",
      """{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":1,"Stage Attempt ID":0,"Stage Name":"stage-1","Number of Tasks":1,"RDD Info":[],"Parent IDs":[],"Details":"stage-1 details","Submission Time":2000,"Completion Time":2100,"Accumulables":[],"Resource Profile Id":0}}""",
      """{"Event":"SparkListenerJobEnd","Job ID":1,"Completion Time":2200,"Job Result":{"Result":"JobSucceeded"}}""",
      """{"Event":"SparkListenerApplicationEnd","Timestamp":2300}"""
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val hadoopConf = new Configuration()
    EventLogStreamingParser.writeUIMeta(
      eventLog = eventLog.toUri.toString,
      uimetaDir = uimetaDir.toUri.toString,
      attemptIdOverride = None,
      compression = "none",
      taskShardRecords = 100000,
      hadoopConf = hadoopConf)

    val provider = new UIMetaProvider(new SparkConf()
      .set("spark.uimeta.dir", uimetaDir.toUri.toString)
      .set("spark.history.fs.logDirectory", uimetaDir.toUri.toString))
    try {
      val loaded = provider.getAppUI("local-provider-v2", None)

      assert(loaded.isDefined)
      val store = kvStore(loaded.get.ui.store).asInstanceOf[UIMetaShardStore]
      assert(!store.isKindLoaded("jobs"))
      assert(!store.isKindLoaded("stages"))
      assert(store.count(classOf[JobDataWrapper]) == 1L)
      assert(store.isKindLoaded("jobs"))
      assert(!store.isKindLoaded("stages"))
      assert(store.count(classOf[StageDataWrapper]) == 1L)
      assert(store.isKindLoaded("stages"))
      assert(loaded.get.ui.store.taskCount(1, 0) == 0L)
    } finally {
      provider.stop()
    }
  }

  test("provider loads task shards only when a stage task query runs") {
    val tempDir = Files.createTempDirectory("uimeta-provider-v2-tasks")
    val eventLog = tempDir.resolve("local-provider-v2-tasks")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val lines = Seq(
      """{"Event":"SparkListenerApplicationStart","App Name":"Provider V2 Tasks","App ID":"local-provider-v2-tasks","Timestamp":1000,"User":"alice"}""",
      """{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":2,"Stage Attempt ID":0,"Stage Name":"stage-2","Number of Tasks":1,"RDD Info":[],"Parent IDs":[],"Details":"stage-2 details","Submission Time":2000,"Accumulables":[],"Resource Profile Id":0},"Properties":{}}""",
      """{"Event":"SparkListenerTaskEnd","Stage ID":2,"Stage Attempt ID":0,"Task Type":"ResultTask","Task End Reason":{"Reason":"Success"},"Task Info":{"Task ID":10,"Index":0,"Attempt":0,"Partition ID":0,"Launch Time":2050,"Finish Time":2150,"Executor ID":"1","Host":"localhost","Locality":"PROCESS_LOCAL","Speculative":false,"Getting Result Time":0,"Failed":false,"Killed":false,"Accumulables":[]},"Task Metrics":{"Executor Deserialize Time":1,"Executor Deserialize CPU Time":2,"Executor Run Time":10,"Executor CPU Time":20,"Result Size":30,"JVM GC Time":0,"Result Serialization Time":1,"Memory Bytes Spilled":0,"Disk Bytes Spilled":0,"Peak Execution Memory":0}}""",
      """{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":2,"Stage Attempt ID":0,"Stage Name":"stage-2","Number of Tasks":1,"RDD Info":[],"Parent IDs":[],"Details":"stage-2 details","Submission Time":2000,"Completion Time":2200,"Accumulables":[],"Resource Profile Id":0}}""",
      """{"Event":"SparkListenerApplicationEnd","Timestamp":2300}"""
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val hadoopConf = new Configuration()
    EventLogStreamingParser.writeUIMeta(
      eventLog = eventLog.toUri.toString,
      uimetaDir = uimetaDir.toUri.toString,
      attemptIdOverride = None,
      compression = "none",
      taskShardRecords = 100000,
      hadoopConf = hadoopConf)

    val provider = new UIMetaProvider(new SparkConf()
      .set("spark.uimeta.dir", uimetaDir.toUri.toString)
      .set("spark.history.fs.logDirectory", uimetaDir.toUri.toString))
    try {
      val loaded = provider.getAppUI("local-provider-v2-tasks", None)
      assert(loaded.isDefined)

      val store = kvStore(loaded.get.ui.store)
      assert(store.count(classOf[TaskDataWrapper]) == 0L)
      assert(loaded.get.ui.store.taskList(2, 0, 10).map(_.taskId) == Seq(10L))
      assert(loaded.get.ui.store.taskSummary(2, 0, Array(0.5)).isDefined)
      assert(store.count(classOf[TaskDataWrapper]) == 1L)
    } finally {
      provider.stop()
    }
  }

  test("provider attaches SQL tab when v2 SQL metadata exists") {
    val tempDir = Files.createTempDirectory("uimeta-provider-v2-sql")
    val eventLog = tempDir.resolve("local-provider-v2-sql")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val lines = Seq(
      """{"Event":"SparkListenerApplicationStart","App Name":"Provider V2 SQL","App ID":"local-provider-v2-sql","Timestamp":1000,"User":"alice"}""",
      sqlExecutionStartJson(99L),
      """{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd","executionId":99,"time":1200}""",
      """{"Event":"SparkListenerApplicationEnd","Timestamp":1300}"""
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val hadoopConf = new Configuration()
    EventLogStreamingParser.writeUIMeta(
      eventLog = eventLog.toUri.toString,
      uimetaDir = uimetaDir.toUri.toString,
      attemptIdOverride = None,
      compression = "none",
      taskShardRecords = 100000,
      hadoopConf = hadoopConf)

    val provider = new UIMetaProvider(new SparkConf()
      .set("spark.uimeta.dir", uimetaDir.toUri.toString)
      .set("spark.history.fs.logDirectory", uimetaDir.toUri.toString))
    try {
      val loaded = provider.getAppUI("local-provider-v2-sql", None)
      assert(loaded.isDefined)
      assert(kvStore(loaded.get.ui.store).count(classOf[SQLExecutionUIData]) == 1L)
      assert(loaded.get.ui.getTabs.exists(_.prefix == "SQL"))
    } finally {
      provider.stop()
    }
  }

  private def kvStore(appStatusStore: org.apache.spark.status.AppStatusStore): KVStore = {
    appStatusStore.store
  }

  private def sqlExecutionStartJson(executionId: Long): String = {
    s"""{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart","executionId":$executionId,"description":"select 1","details":"select 1","physicalPlanDescription":"LocalTableScan","sparkPlanInfo":{"nodeName":"LocalTableScan","simpleString":"LocalTableScan","children":[],"metadata":{},"metrics":[]},"time":1100,"modifiedConfigs":{"spark.sql.shuffle.partitions":"1"}}"""
  }
}
