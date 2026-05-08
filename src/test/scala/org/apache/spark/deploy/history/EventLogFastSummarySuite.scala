package org.apache.spark.deploy.history

import java.io.DataInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.spark.status.{JobDataWrapper, StageDataWrapper, TaskDataWrapper}
import org.apache.spark.util.kvstore.InMemoryStore
import org.scalatest.funsuite.AnyFunSuite

class EventLogFastSummarySuite extends AnyFunSuite {

  test("fast summary writes app, job, and stage metadata while skipping task details") {
    val tempDir = Files.createTempDirectory("fast-summary-suite")
    val eventLog = tempDir.resolve("local-1")
    val uimetaDir = tempDir.resolve("uimeta")
    Files.createDirectories(uimetaDir)

    val lines = Seq(
      """{"Event":"SparkListenerEnvironmentUpdate","JVM Information":{"Java Version":"11.0.20","Java Home":"/java","Scala Version":"version 2.12.15"},"Spark Properties":{"spark.app.id":"local-1","spark.app.name":"Fast App","spark.app.attempt.id":"attempt-42","spark.executor.memory":"4g"},"Hadoop Properties":{},"System Properties":{},"Classpath Entries":{}}""",
      """{"Event":"SparkListenerApplicationStart","App Name":"Fast App","App ID":"local-1","Timestamp":1000,"User":"alice"}""",
      """{"Event":"SparkListenerJobStart","Job ID":7,"Submission Time":2000,"Stage Infos":[{"Stage ID":3,"Stage Attempt ID":0,"Stage Name":"map stage","Number of Tasks":5,"RDD Info":[{"RDD ID":10}],"Parent IDs":[],"Details":"map details","Submission Time":2100,"Resource Profile Id":0},{"Stage ID":4,"Stage Attempt ID":0,"Stage Name":"reduce stage","Number of Tasks":2,"RDD Info":[],"Parent IDs":[3],"Details":"reduce details","Resource Profile Id":0}],"Stage IDs":[3,4],"Properties":{"spark.job.description":"important job","spark.jobGroup.id":"group-a","spark.sql.execution.id":"99"}}""",
      """{"Event":"SparkListenerStageSubmitted","Stage Info":{"Stage ID":3,"Stage Attempt ID":0,"Stage Name":"map stage","Number of Tasks":5,"RDD Info":[{"RDD ID":10}],"Parent IDs":[],"Details":"map details","Submission Time":2100,"Accumulables":[],"Resource Profile Id":0},"Properties":{}}""",
      """{"Event":"SparkListenerTaskEnd","Task Info": this task payload is intentionally invalid json and must not be parsed}""",
      """{"Event":"SparkListenerStageCompleted","Stage Info":{"Stage ID":3,"Stage Attempt ID":0,"Stage Name":"map stage","Number of Tasks":5,"RDD Info":[{"RDD ID":10}],"Parent IDs":[],"Details":"map details","Submission Time":2100,"Completion Time":2600,"Accumulables":[{"Name":"internal.metrics.executorRunTime","Value":123},{"Name":"internal.metrics.input.recordsRead","Value":456},{"Name":"internal.metrics.shuffle.write.bytesWritten","Value":789}],"Resource Profile Id":0}}""",
      """{"Event":"SparkListenerJobEnd","Job ID":7,"Completion Time":3000,"Job Result":{"Result":"JobSucceeded"}}""",
      """{"Event":"SparkListenerApplicationEnd","Timestamp":3500}"""
    )
    Files.write(eventLog, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

    val result = FastEventLogSummary.writeUIMeta(
      eventLog.toUri.toString,
      uimetaDir.toUri.toString,
      None,
      new Configuration())

    assert(result.appId == "local-1")
    assert(result.attemptId.contains("attempt-42"))
    assert(result.mode == "fast-summary")

    val store = loadStore(result.uimetaPath.toString)
    val appStatusStore = SparkUIServiceCompat.createAppStatusStore(store)

    val appInfo = appStatusStore.applicationInfo()
    assert(appInfo.id == "local-1")
    assert(appInfo.name == "Fast App")
    assert(appInfo.attempts.head.sparkUser == "alice")
    assert(appInfo.attempts.head.duration == 2500L)

    val jobs = store.view(classOf[JobDataWrapper]).iterator()
    assert(jobs.hasNext)
    val job = jobs.next().info
    assert(job.jobId == 7)
    assert(job.description.contains("important job"))
    assert(job.stageIds == Seq(3, 4))
    assert(job.numTasks == 7)
    assert(job.numCompletedTasks == 7)

    val stages = store.view(classOf[StageDataWrapper]).iterator()
    val stageInfos = Iterator.continually(stages)
      .takeWhile(_.hasNext)
      .map(_.next().info)
      .toSeq
      .sortBy(_.stageId)
    assert(stageInfos.map(_.stageId) == Seq(3, 4))
    assert(stageInfos.head.numCompleteTasks == 5)
    assert(stageInfos.head.executorRunTime == 123L)
    assert(stageInfos.head.inputRecords == 456L)
    assert(stageInfos.head.shuffleWriteBytes == 789L)

    val tasks = store.view(classOf[TaskDataWrapper]).iterator()
    assert(!tasks.hasNext)
  }

  private def loadStore(path: String): InMemoryStore = {
    val store = new InMemoryStore()
    val in = new DataInputStream(new java.io.FileInputStream(new java.net.URI(path).getPath))
    try {
      assert(UIMetaFile.verifyHeader(in))
      var elem = UIMetaFile.readElementBytes(in)
      while (elem.isDefined) {
        val (className, data) = elem.get
        val clazz = org.apache.spark.util.Utils.classForName(className)
        store.write(UIMetaFile.deserialize(data, clazz))
        elem = UIMetaFile.readElementBytes(in)
      }
      store
    } finally {
      in.close()
    }
  }
}
