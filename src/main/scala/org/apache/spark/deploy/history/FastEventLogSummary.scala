package org.apache.spark.deploy.history

import java.io.{BufferedOutputStream, BufferedReader, DataOutputStream, InputStreamReader}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Date

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataOutputStream, Path}
import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.api.v1._
import org.apache.spark.ui.scope.RDDOperationEdge

private[history] case class EventLogPreprocessResult(
    eventLog: String,
    appId: String,
    attemptId: Option[String],
    uimetaPath: Path,
    historyPath: String,
    mode: String)

private[history] object FastEventLogSummary {

  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m
  }

  private val interestingEvents = Set(
    "SparkListenerApplicationStart",
    "SparkListenerApplicationEnd",
    "SparkListenerEnvironmentUpdate",
    "SparkListenerJobStart",
    "SparkListenerJobEnd",
    "SparkListenerStageSubmitted",
    "SparkListenerStageCompleted")

  def writeUIMeta(
      eventLog: String,
      uimetaDir: String,
      attemptIdOverride: Option[String],
      hadoopConf: Configuration): EventLogPreprocessResult = {
    val summary = parse(eventLog, hadoopConf)
    val attemptId = attemptIdOverride.orElse(summary.attemptId)
    val metaFileName = UIMetaSnapshotWriter.fileName(summary.appId, attemptId)
    val metaPath = FastSummaryUIMetaWriter.write(summary, attemptId, uimetaDir, metaFileName, hadoopConf)
    val historyPath = attemptId match {
      case Some(id) => s"/history/${summary.appId}/$id/jobs/"
      case None => s"/history/${summary.appId}/jobs/"
    }

    EventLogPreprocessResult(eventLog, summary.appId, attemptId, metaPath, historyPath, "fast-summary")
  }

  private def parse(eventLog: String, hadoopConf: Configuration): Summary = {
    val path = new Path(eventLog)
    val fs = path.getFileSystem(hadoopConf)
    val state = new SummaryState(inferAppId(path))

    EventLogFileReader(fs, path) match {
      case Some(reader) =>
        val files = reader.listEventLogFiles
        val lastFile = files.lastOption.map(_.getPath)
        val eventLogInProgress = !reader.completed
        files.foreach { status =>
          val tolerateTruncatedFinalLine = lastFile.contains(status.getPath) &&
            (eventLogInProgress || isInProgressPath(status.getPath))
          scanFile(status, fs, state, tolerateTruncatedFinalLine)
        }
      case None =>
        scanPath(path, fs, state, isInProgressPath(path))
    }

    state.toSummary()
  }

  private def scanFile(
      status: FileStatus,
      fs: FileSystem,
      state: SummaryState,
      tolerateTruncatedFinalLine: Boolean): Unit = {
    scanPath(status.getPath, fs, state, tolerateTruncatedFinalLine)
  }

  private def scanPath(
      path: Path,
      fs: FileSystem,
      state: SummaryState,
      tolerateTruncatedFinalLine: Boolean): Unit = {
    val in = EventLogFileReader.openEventLog(path, fs)
    val reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1024 * 1024)
    try {
      var line = reader.readLine()
      while (line != null) {
        val nextLine = reader.readLine()
        handleLine(line, state, tolerateTruncatedFinalLine && nextLine == null)
        line = nextLine
      }
    } finally {
      reader.close()
    }
  }

  private def handleLine(
      line: String,
      state: SummaryState,
      tolerateMalformedJson: Boolean): Unit = {
    eventName(line) match {
      case Some(name) if name.startsWith("SparkListenerTask") =>
        state.skippedTaskEvents += 1L
      case Some(name) if interestingEvents.contains(name) =>
        try {
          val node = mapper.readTree(line)
          handleEvent(name, node, state)
        } catch {
          case _: JsonProcessingException if tolerateMalformedJson =>
            state.truncatedFinalLine = true
        }
      case _ =>
    }
  }

  private def handleEvent(event: String, node: JsonNode, state: SummaryState): Unit = event match {
    case "SparkListenerApplicationStart" =>
      state.appName = text(node, "App Name").orElse(state.appName)
      state.appId = text(node, "App ID").orElse(state.appId)
      state.attemptId = text(node, "App Attempt ID").orElse(state.attemptId)
      state.startTime = long(node, "Timestamp").orElse(state.startTime)
      state.user = text(node, "User").orElse(state.user)

    case "SparkListenerApplicationEnd" =>
      state.endTime = long(node, "Timestamp").orElse(state.endTime)
      state.completed = true

    case "SparkListenerEnvironmentUpdate" =>
      state.environment = EnvironmentSummary.fromEvent(node)
      state.appId = state.appId
        .orElse(state.environment.sparkProperties.get("spark.app.id"))
      state.appName = state.appName
        .orElse(state.environment.sparkProperties.get("spark.app.name"))
      state.attemptId = state.attemptId
        .orElse(state.environment.sparkProperties.get("spark.app.attempt.id"))

    case "SparkListenerJobStart" =>
      val jobId = int(node, "Job ID").getOrElse(return)
      val job = state.jobs.getOrElseUpdate(jobId, JobSummary(jobId))
      job.submissionTime = long(node, "Submission Time").orElse(job.submissionTime)
      val properties = objectFields(node.get("Properties")).toMap
      job.description = properties.get("spark.job.description").orElse(job.description)
      job.jobGroup = properties.get("spark.jobGroup.id").orElse(job.jobGroup)
      job.sqlExecutionId = properties.get("spark.sql.execution.id").flatMap(parseLong)
        .orElse(job.sqlExecutionId)
      job.status = JobExecutionStatus.RUNNING

      val stageKeys = node.get("Stage Infos") match {
        case stages if stages != null && stages.isArray =>
          stages.elements().asScala.map { stageInfo =>
            val stage = updateStageFromInfo(stageInfo, state, Some(StageStatus.PENDING), Some(jobId))
            stage.key
          }.toSeq
        case _ =>
          Seq.empty
      }
      if (stageKeys.nonEmpty) {
        job.stageKeys = stageKeys
        job.stageIds = stageKeys.map(_.stageId)
      } else {
        job.stageIds = intArray(node.get("Stage IDs"))
      }
      if (job.name.isEmpty) {
        job.name = job.description
          .orElse(job.stageKeys.headOption.flatMap(key => state.stages.get(key).map(_.name)))
          .getOrElse(s"Job $jobId")
      }

    case "SparkListenerJobEnd" =>
      val jobId = int(node, "Job ID").getOrElse(return)
      val job = state.jobs.getOrElseUpdate(jobId, JobSummary(jobId))
      job.completionTime = long(node, "Completion Time").orElse(job.completionTime)
      val result = Option(node.get("Job Result")).flatMap(text(_, "Result")).getOrElse("")
      job.status = if (result == "JobSucceeded") {
        JobExecutionStatus.SUCCEEDED
      } else if (result.nonEmpty) {
        JobExecutionStatus.FAILED
      } else {
        JobExecutionStatus.UNKNOWN
      }

    case "SparkListenerStageSubmitted" =>
      updateStageFromInfo(node.get("Stage Info"), state, Some(StageStatus.ACTIVE), None)

    case "SparkListenerStageCompleted" =>
      updateStageFromInfo(node.get("Stage Info"), state, None, None)

    case _ =>
  }

  private def updateStageFromInfo(
      stageInfo: JsonNode,
      state: SummaryState,
      status: Option[StageStatus],
      jobId: Option[Int]): StageSummary = {
    val stageId = int(stageInfo, "Stage ID").getOrElse(-1)
    val attemptId = int(stageInfo, "Stage Attempt ID").getOrElse(0)
    val key = StageKey(stageId, attemptId)
    val stage = state.stages.getOrElseUpdate(key, StageSummary(key))

    text(stageInfo, "Stage Name").foreach(stage.name = _)
    int(stageInfo, "Number of Tasks").foreach(stage.numTasks = _)
    text(stageInfo, "Details").foreach(stage.details = _)
    long(stageInfo, "Submission Time").foreach(v => stage.submissionTime = Some(v))
    long(stageInfo, "Completion Time").foreach(v => stage.completionTime = Some(v))
    text(stageInfo, "Failure Reason").foreach(v => stage.failureReason = Some(v))
    int(stageInfo, "Resource Profile Id").foreach(stage.resourceProfileId = _)
    stage.parentIds = intArray(stageInfo.get("Parent IDs"))
    stage.rddIds = rddIds(stageInfo.get("RDD Info"))
    jobId.foreach(id => stage.jobIds += id)
    status.foreach { s =>
      if (!stage.isTerminal) {
        stage.status = s
      }
    }
    if (stage.completionTime.isDefined) {
      stage.status = stage.failureReason match {
        case Some(_) => StageStatus.FAILED
        case None => StageStatus.COMPLETE
      }
    }
    updateMetrics(stage, stageInfo.get("Accumulables"))
    stage
  }

  private def updateMetrics(stage: StageSummary, accumulables: JsonNode): Unit = {
    if (accumulables == null || !accumulables.isArray) return

    accumulables.elements().asScala.foreach { acc =>
      val value = long(acc, "Value").getOrElse(0L)
      text(acc, "Name").foreach {
        case "internal.metrics.executorDeserializeTime" =>
          stage.metrics.executorDeserializeTime = value
        case "internal.metrics.executorDeserializeCpuTime" =>
          stage.metrics.executorDeserializeCpuTime = value
        case "internal.metrics.executorRunTime" =>
          stage.metrics.executorRunTime = value
        case "internal.metrics.executorCpuTime" =>
          stage.metrics.executorCpuTime = value
        case "internal.metrics.resultSize" =>
          stage.metrics.resultSize = value
        case "internal.metrics.jvmGCTime" =>
          stage.metrics.jvmGcTime = value
        case "internal.metrics.resultSerializationTime" =>
          stage.metrics.resultSerializationTime = value
        case "internal.metrics.memoryBytesSpilled" =>
          stage.metrics.memoryBytesSpilled = value
        case "internal.metrics.diskBytesSpilled" =>
          stage.metrics.diskBytesSpilled = value
        case "internal.metrics.peakExecutionMemory" =>
          stage.metrics.peakExecutionMemory = value
        case "internal.metrics.input.bytesRead" =>
          stage.metrics.inputBytes = value
        case "internal.metrics.input.recordsRead" =>
          stage.metrics.inputRecords = value
        case "internal.metrics.output.bytesWritten" =>
          stage.metrics.outputBytes = value
        case "internal.metrics.output.recordsWritten" =>
          stage.metrics.outputRecords = value
        case "internal.metrics.shuffle.read.remoteBlocksFetched" =>
          stage.metrics.shuffleRemoteBlocksFetched = value
        case "internal.metrics.shuffle.read.localBlocksFetched" =>
          stage.metrics.shuffleLocalBlocksFetched = value
        case "internal.metrics.shuffle.read.fetchWaitTime" =>
          stage.metrics.shuffleFetchWaitTime = value
        case "internal.metrics.shuffle.read.remoteBytesRead" =>
          stage.metrics.shuffleRemoteBytesRead = value
        case "internal.metrics.shuffle.read.remoteBytesReadToDisk" =>
          stage.metrics.shuffleRemoteBytesReadToDisk = value
        case "internal.metrics.shuffle.read.localBytesRead" =>
          stage.metrics.shuffleLocalBytesRead = value
        case "internal.metrics.shuffle.read.recordsRead" =>
          stage.metrics.shuffleReadRecords = value
        case "internal.metrics.shuffle.write.bytesWritten" =>
          stage.metrics.shuffleWriteBytes = value
        case "internal.metrics.shuffle.write.writeTime" =>
          stage.metrics.shuffleWriteTime = value
        case "internal.metrics.shuffle.write.recordsWritten" =>
          stage.metrics.shuffleWriteRecords = value
        case _ =>
      }
    }
  }

  private def eventName(line: String): Option[String] = {
    val marker = "\"Event\""
    val markerIndex = line.indexOf(marker)
    if (markerIndex < 0) return None
    val colonIndex = line.indexOf(':', markerIndex + marker.length)
    if (colonIndex < 0) return None
    val startQuote = line.indexOf('"', colonIndex + 1)
    if (startQuote < 0) return None
    val endQuote = line.indexOf('"', startQuote + 1)
    if (endQuote < 0) return None
    Some(line.substring(startQuote + 1, endQuote))
  }

  private def text(node: JsonNode, field: String): Option[String] = {
    if (node == null) return None
    val value = node.get(field)
    if (value == null || value.isNull) None else Some(value.asText())
  }

  private def long(node: JsonNode, field: String): Option[Long] = {
    if (node == null) return None
    val value = node.get(field)
    if (value == null || value.isNull) None
    else if (value.isNumber) Some(value.asLong())
    else parseLong(value.asText())
  }

  private def int(node: JsonNode, field: String): Option[Int] = {
    long(node, field).map(_.toInt)
  }

  private def parseLong(value: String): Option[Long] = {
    try {
      Some(value.toLong)
    } catch {
      case _: NumberFormatException => None
    }
  }

  private def objectFields(node: JsonNode): Seq[(String, String)] = {
    if (node == null || !node.isObject) {
      Seq.empty
    } else {
      node.fields().asScala.map { entry =>
        entry.getKey -> entry.getValue.asText()
      }.toSeq
    }
  }

  private def intArray(node: JsonNode): Seq[Int] = {
    if (node == null || !node.isArray) {
      Seq.empty
    } else {
      node.elements().asScala.flatMap { item =>
        if (item.isNumber) Some(item.asInt()) else parseLong(item.asText()).map(_.toInt)
      }.toSeq
    }
  }

  private def rddIds(node: JsonNode): Seq[Int] = {
    if (node == null || !node.isArray) {
      Seq.empty
    } else {
      node.elements().asScala.flatMap(rdd => int(rdd, "RDD ID")).toSeq
    }
  }

  private def inferAppId(path: Path): String = {
    path.getName.stripSuffix(".inprogress")
  }

  private def isInProgressPath(path: Path): Boolean = {
    path.getName.endsWith(".inprogress")
  }

  private class SummaryState(defaultAppId: String) {
    var appId: Option[String] = Some(defaultAppId).filter(_.nonEmpty)
    var appName: Option[String] = None
    var attemptId: Option[String] = None
    var startTime: Option[Long] = None
    var endTime: Option[Long] = None
    var user: Option[String] = None
    var completed: Boolean = false
    var environment: EnvironmentSummary = EnvironmentSummary.empty
    var skippedTaskEvents: Long = 0L
    var truncatedFinalLine: Boolean = false
    val jobs: mutable.LinkedHashMap[Int, JobSummary] = mutable.LinkedHashMap.empty
    val stages: mutable.LinkedHashMap[StageKey, StageSummary] = mutable.LinkedHashMap.empty

    def toSummary(): Summary = {
      finalizeSucceedingJobs()
      val finalAppId = appId.getOrElse(defaultAppId)
      val finalStartTime = startTime
        .orElse(environment.sparkProperties.get("spark.app.startTime").flatMap(parseLong))
        .getOrElse(System.currentTimeMillis())
      val latestCompletion = jobs.values.flatMap(_.completionTime)
        .++(stages.values.flatMap(_.completionTime))
        .toSeq
        .sorted
        .lastOption
      val finalEndTime = endTime.orElse(latestCompletion)
      val finalCompleted = completed || finalEndTime.isDefined
      val finalAppName = appName
        .orElse(environment.sparkProperties.get("spark.app.name"))
        .getOrElse(finalAppId)
      val finalUser = user.getOrElse(System.getProperty("user.name", "unknown"))

      Summary(
        appId = finalAppId,
        appName = finalAppName,
        attemptId = attemptId,
        user = finalUser,
        startTime = finalStartTime,
        endTime = finalEndTime,
        completed = finalCompleted,
        environment = environment,
        jobs = jobs.values.toSeq,
        stages = stages.values.toSeq,
        skippedTaskEvents = skippedTaskEvents)
    }

    private def finalizeSucceedingJobs(): Unit = {
      jobs.values.filter(_.status == JobExecutionStatus.SUCCEEDED).foreach { job =>
        job.stageKeys.foreach { key =>
          stages.get(key).foreach { stage =>
            if (!stage.isTerminal) {
              stage.status = StageStatus.COMPLETE
              stage.completionTime = job.completionTime.orElse(stage.completionTime)
            }
          }
        }
      }
    }
  }

  private case class Summary(
      appId: String,
      appName: String,
      attemptId: Option[String],
      user: String,
      startTime: Long,
      endTime: Option[Long],
      completed: Boolean,
      environment: EnvironmentSummary,
      jobs: Seq[JobSummary],
      stages: Seq[StageSummary],
      skippedTaskEvents: Long)

  private case class StageKey(stageId: Int, attemptId: Int)

  private case class JobSummary(jobId: Int) {
    var name: String = ""
    var description: Option[String] = None
    var submissionTime: Option[Long] = None
    var completionTime: Option[Long] = None
    var stageIds: Seq[Int] = Seq.empty
    var stageKeys: Seq[StageKey] = Seq.empty
    var jobGroup: Option[String] = None
    var status: JobExecutionStatus = JobExecutionStatus.UNKNOWN
    var sqlExecutionId: Option[Long] = None
  }

  private case class StageSummary(key: StageKey) {
    var name: String = s"Stage ${key.stageId}"
    var numTasks: Int = 0
    var submissionTime: Option[Long] = None
    var completionTime: Option[Long] = None
    var failureReason: Option[String] = None
    var details: String = ""
    var parentIds: Seq[Int] = Seq.empty
    var rddIds: Seq[Int] = Seq.empty
    var resourceProfileId: Int = 0
    var status: StageStatus = StageStatus.PENDING
    var jobIds: mutable.Set[Int] = mutable.LinkedHashSet.empty
    val metrics: StageMetrics = StageMetrics()

    def isTerminal: Boolean = status == StageStatus.COMPLETE ||
      status == StageStatus.FAILED ||
      status == StageStatus.SKIPPED
  }

  private case class StageMetrics(
      var executorDeserializeTime: Long = 0L,
      var executorDeserializeCpuTime: Long = 0L,
      var executorRunTime: Long = 0L,
      var executorCpuTime: Long = 0L,
      var resultSize: Long = 0L,
      var jvmGcTime: Long = 0L,
      var resultSerializationTime: Long = 0L,
      var memoryBytesSpilled: Long = 0L,
      var diskBytesSpilled: Long = 0L,
      var peakExecutionMemory: Long = 0L,
      var inputBytes: Long = 0L,
      var inputRecords: Long = 0L,
      var outputBytes: Long = 0L,
      var outputRecords: Long = 0L,
      var shuffleRemoteBlocksFetched: Long = 0L,
      var shuffleLocalBlocksFetched: Long = 0L,
      var shuffleFetchWaitTime: Long = 0L,
      var shuffleRemoteBytesRead: Long = 0L,
      var shuffleRemoteBytesReadToDisk: Long = 0L,
      var shuffleLocalBytesRead: Long = 0L,
      var shuffleReadRecords: Long = 0L,
      var shuffleWriteBytes: Long = 0L,
      var shuffleWriteTime: Long = 0L,
      var shuffleWriteRecords: Long = 0L)

  private case class EnvironmentSummary(
      javaVersion: String,
      javaHome: String,
      scalaVersion: String,
      sparkProperties: Map[String, String],
      hadoopProperties: Map[String, String],
      systemProperties: Map[String, String],
      classpathEntries: Map[String, String])

  private object EnvironmentSummary {
    def empty: EnvironmentSummary = {
      EnvironmentSummary(
        javaVersion = System.getProperty("java.version", "unknown"),
        javaHome = System.getProperty("java.home", ""),
        scalaVersion = util.Properties.versionString,
        sparkProperties = Map.empty,
        hadoopProperties = Map.empty,
        systemProperties = Map.empty,
        classpathEntries = Map.empty)
    }

    def fromEvent(node: JsonNode): EnvironmentSummary = {
      val jvm = Option(node.get("JVM Information"))
      EnvironmentSummary(
        javaVersion = jvm.flatMap(text(_, "Java Version")).getOrElse(empty.javaVersion),
        javaHome = jvm.flatMap(text(_, "Java Home")).getOrElse(empty.javaHome),
        scalaVersion = jvm.flatMap(text(_, "Scala Version")).getOrElse(empty.scalaVersion),
        sparkProperties = objectFields(node.get("Spark Properties")).toMap,
        hadoopProperties = objectFields(node.get("Hadoop Properties")).toMap,
        systemProperties = objectFields(node.get("System Properties")).toMap,
        classpathEntries = objectFields(node.get("Classpath Entries")).toMap)
    }
  }

  private object FastSummaryUIMetaWriter {

    def write(
        summary: Summary,
        attemptId: Option[String],
        logDir: String,
        targetFileName: String,
        hadoopConf: Configuration,
        tempSuffix: String = System.nanoTime().toString): Path = {
      val fs = FileSystem.get(new URI(logDir), hadoopConf)
      val dir = new Path(logDir)
      val targetPath = new Path(dir, targetFileName)
      val tempPath = new Path(dir, s".$targetFileName.tmp.$tempSuffix")
      var tempSt: FSDataOutputStream = null
      var out: DataOutputStream = null

      try {
        if (!fs.exists(dir)) fs.mkdirs(dir)
        tempSt = fs.create(tempPath, true)
        out = new DataOutputStream(new BufferedOutputStream(tempSt))
        UIMetaFile.writeHeader(out)
        writeElements(summary, attemptId, out)
        out.flush()
        out.close()
        out = null

        if (fs.exists(targetPath)) fs.delete(targetPath, false)
        if (!fs.rename(tempPath, targetPath)) {
          throw new IllegalStateException(s"Failed to rename $tempPath to $targetPath")
        }
        targetPath
      } finally {
        if (out != null) out.close()
        else if (tempSt != null) tempSt.close()
        if (fs.exists(tempPath)) fs.delete(tempPath, false)
      }
    }

    private def writeElements(
        summary: Summary,
        attemptId: Option[String],
        out: DataOutputStream): Unit = {
      UIMetaFile.writeElement(
        out,
        classOf[org.apache.spark.status.ApplicationInfoWrapper].getName,
        new org.apache.spark.status.ApplicationInfoWrapper(toApplicationInfo(summary, attemptId)))
      UIMetaFile.writeElement(
        out,
        classOf[org.apache.spark.status.ApplicationEnvironmentInfoWrapper].getName,
        new org.apache.spark.status.ApplicationEnvironmentInfoWrapper(toEnvironmentInfo(summary.environment)))

      summary.stages.sortBy(s => (s.key.stageId, s.key.attemptId)).foreach { stage =>
        UIMetaFile.writeElement(
          out,
          classOf[org.apache.spark.status.StageDataWrapper].getName,
          new org.apache.spark.status.StageDataWrapper(toStageData(stage), stage.jobIds.toSet, Map.empty))
        UIMetaFile.writeElement(
          out,
          classOf[org.apache.spark.status.RDDOperationGraphWrapper].getName,
          new org.apache.spark.status.RDDOperationGraphWrapper(
            stage.key.stageId,
            Seq.empty[RDDOperationEdge],
            Seq.empty[RDDOperationEdge],
            Seq.empty[RDDOperationEdge],
            new org.apache.spark.status.RDDOperationClusterWrapper(
              s"stage-${stage.key.stageId}",
              stage.name,
              Seq.empty,
              Seq.empty)))
      }

      summary.jobs.sortBy(_.jobId).foreach { job =>
        UIMetaFile.writeElement(
          out,
          classOf[org.apache.spark.status.JobDataWrapper].getName,
          new org.apache.spark.status.JobDataWrapper(toJobData(job, summary), Set.empty, job.sqlExecutionId))
      }

      UIMetaFile.writeElement(
        out,
        classOf[org.apache.spark.status.AppSummary].getName,
        new org.apache.spark.status.AppSummary(
          summary.jobs.count(_.status == JobExecutionStatus.SUCCEEDED),
          summary.stages.count(_.status == StageStatus.COMPLETE)))
    }

    private def toApplicationInfo(summary: Summary, attemptId: Option[String]): ApplicationInfo = {
      val start = new Date(summary.startTime)
      val end = summary.endTime.map(new Date(_)).getOrElse(new Date(-1L))
      val duration = summary.endTime.map(t => math.max(0L, t - summary.startTime)).getOrElse(0L)
      val attempt = ApplicationAttemptInfo(
        attemptId = attemptId,
        startTime = start,
        endTime = end,
        lastUpdated = summary.endTime.map(new Date(_)).getOrElse(start),
        duration = duration,
        sparkUser = summary.user,
        completed = summary.completed,
        appSparkVersion = org.apache.spark.SPARK_VERSION)

      ApplicationInfo(
        id = summary.appId,
        name = summary.appName,
        coresGranted = None,
        maxCores = summary.environment.sparkProperties.get("spark.cores.max").flatMap(parseInt),
        coresPerExecutor = summary.environment.sparkProperties.get("spark.executor.cores").flatMap(parseInt),
        memoryPerExecutorMB = summary.environment.sparkProperties.get("spark.executor.memory").flatMap(parseMemoryMb),
        attempts = Seq(attempt))
    }

    private def toEnvironmentInfo(environment: EnvironmentSummary): ApplicationEnvironmentInfo = {
      new ApplicationEnvironmentInfo(
        new RuntimeInfo(environment.javaVersion, environment.javaHome, environment.scalaVersion),
        environment.sparkProperties.toSeq,
        environment.hadoopProperties.toSeq,
        environment.systemProperties.toSeq,
        environment.classpathEntries.toSeq,
        Seq.empty)
    }

    private def toJobData(job: JobSummary, summary: Summary): JobData = {
      val stageMap = summary.stages.map(stage => stage.key -> stage).toMap
      val stages = job.stageKeys.flatMap(stageMap.get)
      val numTasks = stages.map(_.numTasks).sum
      val completedStages = stages.count(_.status == StageStatus.COMPLETE)
      val failedStages = stages.count(_.status == StageStatus.FAILED)
      val skippedStages = if (job.status == JobExecutionStatus.SUCCEEDED) {
        math.max(0, job.stageIds.size - completedStages)
      } else {
        0
      }
      val completedTasks = if (job.status == JobExecutionStatus.SUCCEEDED) {
        numTasks
      } else {
        stages.filter(_.status == StageStatus.COMPLETE).map(_.numTasks).sum
      }
      val failedTasks = stages.filter(_.status == StageStatus.FAILED).map(_.numTasks).sum
      val activeStages = if (job.status == JobExecutionStatus.RUNNING) {
        stages.count(s => s.status == StageStatus.ACTIVE || s.status == StageStatus.PENDING)
      } else {
        0
      }

      new JobData(
        jobId = job.jobId,
        name = if (job.name.nonEmpty) job.name else s"Job ${job.jobId}",
        description = job.description,
        submissionTime = job.submissionTime.map(new Date(_)),
        completionTime = job.completionTime.map(new Date(_)),
        stageIds = job.stageIds,
        jobGroup = job.jobGroup,
        status = job.status,
        numTasks = numTasks,
        numActiveTasks = if (job.status == JobExecutionStatus.RUNNING) math.max(0, numTasks - completedTasks) else 0,
        numCompletedTasks = completedTasks,
        numSkippedTasks = stages.filter(_.status == StageStatus.SKIPPED).map(_.numTasks).sum,
        numFailedTasks = failedTasks,
        numKilledTasks = 0,
        numCompletedIndices = completedTasks,
        numActiveStages = activeStages,
        numCompletedStages = completedStages,
        numSkippedStages = skippedStages,
        numFailedStages = failedStages,
        killedTasksSummary = Map.empty)
    }

    private def toStageData(stage: StageSummary): StageData = {
      val metrics = stage.metrics
      val completedTasks = if (stage.status == StageStatus.COMPLETE) stage.numTasks else 0
      val failedTasks = if (stage.status == StageStatus.FAILED) stage.numTasks else 0
      val activeTasks = if (stage.status == StageStatus.ACTIVE) stage.numTasks else 0
      new StageData(
        status = stage.status,
        stageId = stage.key.stageId,
        attemptId = stage.key.attemptId,
        numTasks = stage.numTasks,
        numActiveTasks = activeTasks,
        numCompleteTasks = completedTasks,
        numFailedTasks = failedTasks,
        numKilledTasks = 0,
        numCompletedIndices = completedTasks,
        submissionTime = stage.submissionTime.map(new Date(_)),
        firstTaskLaunchedTime = None,
        completionTime = stage.completionTime.map(new Date(_)),
        failureReason = stage.failureReason,
        executorDeserializeTime = metrics.executorDeserializeTime,
        executorDeserializeCpuTime = metrics.executorDeserializeCpuTime,
        executorRunTime = metrics.executorRunTime,
        executorCpuTime = metrics.executorCpuTime,
        resultSize = metrics.resultSize,
        jvmGcTime = metrics.jvmGcTime,
        resultSerializationTime = metrics.resultSerializationTime,
        memoryBytesSpilled = metrics.memoryBytesSpilled,
        diskBytesSpilled = metrics.diskBytesSpilled,
        peakExecutionMemory = metrics.peakExecutionMemory,
        inputBytes = metrics.inputBytes,
        inputRecords = metrics.inputRecords,
        outputBytes = metrics.outputBytes,
        outputRecords = metrics.outputRecords,
        shuffleRemoteBlocksFetched = metrics.shuffleRemoteBlocksFetched,
        shuffleLocalBlocksFetched = metrics.shuffleLocalBlocksFetched,
        shuffleFetchWaitTime = metrics.shuffleFetchWaitTime,
        shuffleRemoteBytesRead = metrics.shuffleRemoteBytesRead,
        shuffleRemoteBytesReadToDisk = metrics.shuffleRemoteBytesReadToDisk,
        shuffleLocalBytesRead = metrics.shuffleLocalBytesRead,
        shuffleReadBytes = metrics.shuffleRemoteBytesRead + metrics.shuffleLocalBytesRead,
        shuffleReadRecords = metrics.shuffleReadRecords,
        shuffleWriteBytes = metrics.shuffleWriteBytes,
        shuffleWriteTime = metrics.shuffleWriteTime,
        shuffleWriteRecords = metrics.shuffleWriteRecords,
        name = stage.name,
        description = None,
        details = stage.details,
        schedulingPool = "default",
        rddIds = stage.rddIds,
        accumulatorUpdates = Seq.empty,
        tasks = None,
        executorSummary = None,
        speculationSummary = None,
        killedTasksSummary = Map.empty,
        resourceProfileId = stage.resourceProfileId,
        peakExecutorMetrics = None,
        taskMetricsDistributions = None,
        executorMetricsDistributions = None)
    }

    private def parseInt(value: String): Option[Int] = {
      try {
        Some(value.toInt)
      } catch {
        case _: NumberFormatException => None
      }
    }

    private def parseMemoryMb(value: String): Option[Int] = {
      val lower = value.trim.toLowerCase
      val Pattern = """^(\d+)([kmgt]?)b?$""".r
      lower match {
        case Pattern(amount, unit) =>
          val base = amount.toLong
          val mb = unit match {
            case "k" => math.max(1L, base / 1024L)
            case "" | "m" => base
            case "g" => base * 1024L
            case "t" => base * 1024L * 1024L
          }
          Some(math.min(Int.MaxValue.toLong, mb).toInt)
        case _ =>
          None
      }
    }
  }
}
