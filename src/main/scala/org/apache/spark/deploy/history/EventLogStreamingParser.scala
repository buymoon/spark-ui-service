package org.apache.spark.deploy.history

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.Date

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.JobExecutionStatus
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.status.{ExecutorStageSummaryWrapper, RDDOperationClusterWrapper}
import org.apache.spark.status.api.v1._
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.{SQLMetricInfo, SQLMetrics}
import org.apache.spark.sql.execution.ui.{
  SQLExecutionUIData,
  SQLPlanMetric,
  SparkPlanGraph,
  SparkPlanGraphCluster,
  SparkPlanGraphClusterWrapper,
  SparkPlanGraphNode,
  SparkPlanGraphNodeWrapper,
  SparkPlanGraphWrapper
}
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.scope.{RDDOperationEdge, RDDOperationNode}

private[history] object EventLogStreamingParser extends Logging {

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

  private val SqlAccumulatorMetadata = "sql"
  private val SqlRetainedExecutionsConf = "spark.sql.ui.retainedExecutions"
  private val DefaultSqlRetainedExecutions = Int.MaxValue
  private val DefaultLogProgressInterval = 50000
  private val SummaryShardRecords = 5000
  private val sqlExecutionStartEvents = Set(
    "SparkListenerSQLExecutionStart",
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart")
  private val sqlExecutionEndEvents = Set(
    "SparkListenerSQLExecutionEnd",
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd")
  private val sqlAdaptiveExecutionUpdateEvents = Set(
    "SparkListenerSQLAdaptiveExecutionUpdate",
    "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate")
  private val sqlAdaptiveMetricUpdateEvents = Set(
    "SparkListenerSQLAdaptiveSQLMetricUpdates",
    "org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveSQLMetricUpdates")
  private val sqlDriverAccumUpdateEvents = Set(
    "SparkListenerDriverAccumUpdates",
    "org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates")

  def writeUIMeta(
      eventLog: String,
      uimetaDir: String,
      attemptIdOverride: Option[String],
      compression: String,
      taskShardRecords: Int,
      hadoopConf: Configuration): EventLogPreprocessResult = {
    require(taskShardRecords > 0, "taskShardRecords must be positive")
    UIMetaV2Compression.validate(UIMetaV2Compression.normalize(compression), hadoopConf)

    val parsed = parse(
      eventLog,
      uimetaDir,
      attemptIdOverride,
      compression,
      taskShardRecords,
      hadoopConf)
    val summary = parsed.summary
    logInfo(s"[stream-v2] Parse completed: ${summary.stages.size} stages, " +
      s"${summary.jobs.size} jobs, ${summary.sqlExecutions.size} SQL executions, " +
      s"${parsed.totalTasks} total tasks")
    val attemptId = attemptIdOverride.orElse(summary.attemptId)
    val writer = parsed.writerFor(summary, attemptId)

    logInfo(s"[stream-v2] Writing summary shards...")
    writeSummaryShards(writer, summary, attemptId)
    logInfo(s"[stream-v2] Summary shards written, committing manifest...")
    val metaPath = writer.commit(UIMetaV2Counts(
      jobs = summary.jobs.size,
      stages = summary.stages.size,
      tasks = parsed.totalTasks,
      sqlExecutions = summary.sqlExecutions.count(_.graph.isDefined)),
      completedOverride = Some(summary.completed))
    val historyPath = attemptId match {
      case Some(id) => s"/history/${summary.appId}/$id/jobs/"
      case None => s"/history/${summary.appId}/jobs/"
    }

    EventLogPreprocessResult(eventLog, summary.appId, attemptId, metaPath, historyPath, "stream-v2")
  }

  private def parse(
      eventLog: String,
      uimetaDir: String,
      attemptIdOverride: Option[String],
      compression: String,
      taskShardRecords: Int,
      hadoopConf: Configuration): ParsedEventLog = {
    val path = new Path(eventLog)
    val fs = path.getFileSystem(hadoopConf)
    val state = new SummaryState(inferAppId(path))
    val context = new ParseContext(
      state,
      eventLog,
      uimetaDir,
      attemptIdOverride,
      compression,
      taskShardRecords,
      hadoopConf)

    EventLogFileReader(fs, path) match {
      case Some(reader) =>
        val files = reader.listEventLogFiles
        val lastFile = files.lastOption.map(_.getPath)
        val eventLogInProgress = !reader.completed
        files.foreach { status =>
          val tolerateTruncatedFinalLine = lastFile.contains(status.getPath) &&
            (eventLogInProgress || isInProgressPath(status.getPath))
          scanFile(status, fs, context, tolerateTruncatedFinalLine)
        }
      case None =>
        scanPath(path, fs, context, isInProgressPath(path))
    }

    context.flushTasks()
    ParsedEventLog(state.toSummary(), context.writerOpt, context.totalTasks, context.newWriter)
  }

  private def scanFile(
      status: FileStatus,
      fs: FileSystem,
      context: ParseContext,
      tolerateTruncatedFinalLine: Boolean): Unit = {
    scanPath(status.getPath, fs, context, tolerateTruncatedFinalLine)
  }

  private def scanPath(
      path: Path,
      fs: FileSystem,
      context: ParseContext,
      tolerateTruncatedFinalLine: Boolean): Unit = {
    val in = EventLogFileReader.openEventLog(path, fs)
    val reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8), 1024 * 1024)
    try {
      var lineCount = 0L
      var lastLogLine = 0L
      var line = reader.readLine()
      while (line != null) {
        lineCount += 1L
        val nextLine = reader.readLine()
        handleLine(line, context, tolerateTruncatedFinalLine && nextLine == null)
        if (lineCount - lastLogLine >= DefaultLogProgressInterval) {
          logInfo(s"[stream-v2] Parsed $lineCount lines (tasks=${context.totalTasks}, " +
            s"stages=${context.state.stages.size}, jobs=${context.state.jobs.size}, " +
            s"sql=${context.state.sqlExecutions.size})")
          lastLogLine = lineCount
        }
        line = nextLine
      }
      logInfo(s"[stream-v2] Finished parsing: $lineCount lines total (tasks=${context.totalTasks}, " +
        s"stages=${context.state.stages.size}, jobs=${context.state.jobs.size}, " +
        s"sql=${context.state.sqlExecutions.size})")
    } finally {
      reader.close()
    }
  }

  private def handleLine(
      line: String,
      context: ParseContext,
      tolerateMalformedJson: Boolean): Unit = {
    eventName(line) match {
      case Some("SparkListenerTaskEnd") =>
        context.state.skippedTaskEvents += 1L
        try {
          val node = mapper.readTree(line)
          handleTaskEnd(node, context)
        } catch {
          case _: JsonProcessingException if tolerateMalformedJson =>
            context.state.truncatedFinalLine = true
        }
      case Some(name) if sqlExecutionStartEvents.contains(name) =>
        try {
          val node = mapper.readTree(line)
          handleSqlExecutionStart(node, context.state)
        } catch {
          case _: JsonProcessingException if tolerateMalformedJson =>
            context.state.truncatedFinalLine = true
        }
      case Some(name) if sqlExecutionEndEvents.contains(name) =>
        try {
          val node = mapper.readTree(line)
          handleSqlExecutionEnd(node, context.state)
        } catch {
          case _: JsonProcessingException if tolerateMalformedJson =>
            context.state.truncatedFinalLine = true
        }
      case Some(name) if sqlAdaptiveExecutionUpdateEvents.contains(name) =>
        try {
          val node = mapper.readTree(line)
          handleSqlAdaptiveExecutionUpdate(node, context.state)
        } catch {
          case _: JsonProcessingException if tolerateMalformedJson =>
            context.state.truncatedFinalLine = true
        }
      case Some(name) if sqlAdaptiveMetricUpdateEvents.contains(name) =>
        try {
          val node = mapper.readTree(line)
          handleSqlAdaptiveMetricUpdate(node, context.state)
        } catch {
          case _: JsonProcessingException if tolerateMalformedJson =>
            context.state.truncatedFinalLine = true
        }
      case Some(name) if sqlDriverAccumUpdateEvents.contains(name) =>
        try {
          val node = mapper.readTree(line)
          handleSqlDriverAccumUpdate(node, context.state)
        } catch {
          case _: JsonProcessingException if tolerateMalformedJson =>
            context.state.truncatedFinalLine = true
        }
      case Some(name) if name.startsWith("SparkListenerTask") =>
        context.state.skippedTaskEvents += 1L
      case Some(name) if interestingEvents.contains(name) =>
        try {
          val node = mapper.readTree(line)
          handleEvent(name, node, context)
        } catch {
          case _: JsonProcessingException if tolerateMalformedJson =>
            context.state.truncatedFinalLine = true
        }
      case _ =>
    }
  }

  private def handleSqlExecutionStart(node: JsonNode, state: SummaryState): Unit = {
    val executionId = long(node, "executionId").getOrElse(return)
    sparkPlanInfo(node.get("sparkPlanInfo")).foreach { planInfo =>
      val graph = SparkPlanGraph(planInfo)
      val execution = state.getOrCreateSqlExecution(executionId)
      execution.description = text(node, "description").getOrElse("")
      execution.details = text(node, "details").getOrElse("")
      execution.physicalPlanDescription = text(node, "physicalPlanDescription").getOrElse("")
      execution.modifiedConfigs = objectFields(node.get("modifiedConfigs")).toMap
      execution.metrics = graph.allNodes.flatMap(_.metrics).map { metric =>
        metric.accumulatorId -> metric
      }.toMap.values.toSeq
      execution.submissionTime = long(node, "time").getOrElse(-1L)
      execution.graph = Some(new SparkPlanGraphWrapper(
        executionId,
        toStoredSqlNodes(graph.nodes),
        graph.edges))
      state.pruneSqlExecutions()
    }
  }

  private def handleSqlExecutionEnd(node: JsonNode, state: SummaryState): Unit = {
    val executionId = long(node, "executionId").getOrElse(return)
    state.sqlExecutions.get(executionId).foreach { execution =>
      execution.completionTime = long(node, "time").map(new Date(_))
      state.pruneSqlExecutions()
    }
  }

  private def handleSqlAdaptiveExecutionUpdate(node: JsonNode, state: SummaryState): Unit = {
    val executionId = long(node, "executionId").getOrElse(return)
    sparkPlanInfo(node.get("sparkPlanInfo")).foreach { planInfo =>
      val graph = SparkPlanGraph(planInfo)
      val execution = state.getOrCreateSqlExecution(executionId)
      execution.physicalPlanDescription =
        text(node, "physicalPlanDescription").getOrElse(execution.physicalPlanDescription)
      execution.metrics = mergeSqlMetrics(
        execution.metrics,
        graph.allNodes.flatMap(_.metrics))
      execution.graph = Some(new SparkPlanGraphWrapper(
        executionId,
        toStoredSqlNodes(graph.nodes),
        graph.edges))
      state.pruneSqlExecutions()
    }
  }

  private def handleSqlAdaptiveMetricUpdate(node: JsonNode, state: SummaryState): Unit = {
    val executionId = long(node, "executionId").getOrElse(return)
    val metrics = node.get("sqlPlanMetrics") match {
      case values if values != null && values.isArray =>
        values.elements().asScala.flatMap(sqlPlanMetric).toSeq
      case _ =>
        Seq.empty
    }
    if (metrics.nonEmpty) {
      val execution = state.getOrCreateSqlExecution(executionId)
      execution.metrics = mergeSqlMetrics(execution.metrics, metrics)
      state.pruneSqlExecutions()
    }
  }

  private def handleSqlDriverAccumUpdate(node: JsonNode, state: SummaryState): Unit = {
    val executionId = long(node, "executionId").getOrElse(return)
    val updates = tupleLongLongArray(node.get("accumUpdates"))
    if (updates.nonEmpty) {
      val execution = state.getOrCreateSqlExecution(executionId)
      execution.driverAccumUpdates ++= updates
      state.pruneSqlExecutions()
    }
  }

  private def handleTaskEnd(node: JsonNode, context: ParseContext): Unit = {
    val stageId = int(node, "Stage ID").getOrElse(return)
    val attemptId = int(node, "Stage Attempt ID").getOrElse(0)
    toTaskDataWrapper(node, stageId, attemptId).foreach { task =>
      context.state.recordTaskEnd(StageKey(stageId, attemptId), task)
      context.taskBuffer.append(StageAttemptKey(stageId, attemptId), task)
    }
    handleSqlTaskMetrics(node, stageId, attemptId, context.state)
  }

  private def handleEvent(event: String, node: JsonNode, context: ParseContext): Unit = {
    val state = context.state
    event match {
      case "SparkListenerApplicationStart" =>
        val eventAppId = text(node, "App ID")
        val eventAppName = text(node, "App Name")
        state.appName = eventAppName.orElse(state.appName)
        if (!state.pathMetadataFrozen) {
          state.appId = eventAppId.orElse(state.appId)
          state.attemptId = text(node, "App Attempt ID").orElse(state.attemptId)
        }
        state.startTime = long(node, "Timestamp").orElse(state.startTime)
        state.user = text(node, "User").orElse(state.user)
        if (eventAppId.isDefined) {
          state.authoritativeAppIdSeen = true
        }

      case "SparkListenerApplicationEnd" =>
        state.endTime = long(node, "Timestamp").orElse(state.endTime)
        state.completed = true

      case "SparkListenerEnvironmentUpdate" =>
        state.environment = EnvironmentSummary.fromEvent(node)
        state.environment.sparkProperties.get(SqlRetainedExecutionsConf)
          .flatMap(parseInt)
          .foreach { retained => state.sqlRetainedExecutions = math.max(1, retained) }
        state.pruneSqlExecutions()
        state.environment.sparkProperties.get("spark.app.id").foreach { appId =>
          if (!state.pathMetadataFrozen && !state.authoritativeAppIdSeen) {
            state.appId = Some(appId)
          }
          state.authoritativeAppIdSeen = true
        }
        state.environment.sparkProperties.get("spark.app.name").foreach { appName =>
          state.appName = Some(appName)
        }
        if (!state.pathMetadataFrozen) {
          state.attemptId = state.attemptId
            .orElse(state.environment.sparkProperties.get("spark.app.attempt.id"))
        }

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
              val stage = updateStageFromInfo(
                stageInfo,
                state,
                Some(StageStatus.PENDING),
                Some(jobId),
                Map.empty)
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
        state.registerSqlJob(job)

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
        updateStageFromInfo(
          node.get("Stage Info"),
          state,
          Some(StageStatus.ACTIVE),
          None,
          objectFields(node.get("Properties")).toMap)

      case "SparkListenerStageCompleted" =>
        val stage = updateStageFromInfo(
          node.get("Stage Info"),
          state,
          None,
          None,
          Map.empty)
        context.taskBuffer.flush(StageAttemptKey(stage.key.stageId, stage.key.attemptId))

      case _ =>
    }
  }

  private def updateStageFromInfo(
      stageInfo: JsonNode,
      state: SummaryState,
      status: Option[StageStatus],
      jobId: Option[Int],
      properties: Map[String, String]): StageSummary = {
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
    stage.rddInfos = rddInfos(stageInfo.get("RDD Info"))
    stage.rddIds = stage.rddInfos.map(_.id)
    properties.get("spark.job.description").foreach(v => stage.description = Some(v))
    properties.get("spark.scheduler.pool").foreach(stage.schedulingPool = _)
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
    updateStageAccumulators(stage, stageInfo.get("Accumulables"))
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

  private def updateStageAccumulators(stage: StageSummary, accumulables: JsonNode): Unit = {
    if (accumulables == null || !accumulables.isArray) return
    val updates = accumulables.elements().asScala.flatMap(toTaskAccumulatorUpdate).toSeq
    if (updates.nonEmpty) {
      stage.accumulatorUpdates = updates
    }
  }

  private def toTaskDataWrapper(
      node: JsonNode,
      stageId: Int,
      stageAttemptId: Int): Option[org.apache.spark.status.TaskDataWrapper] = {
    val taskInfo = node.get("Task Info")
    if (taskInfo == null || !taskInfo.isObject) return None

    val taskId = long(taskInfo, "Task ID").getOrElse(return None)
    val index = int(taskInfo, "Index").getOrElse(0)
    val attempt = int(taskInfo, "Attempt").getOrElse(1)
    val partitionId = int(taskInfo, "Partition ID").getOrElse(-1)
    val launchTime = long(taskInfo, "Launch Time").getOrElse(0L)
    val finishTime = long(taskInfo, "Finish Time").getOrElse(launchTime)
    val gettingResultTime = long(taskInfo, "Getting Result Time").filter(_ > 0).getOrElse(0L)
    val duration = math.max(0L, finishTime - launchTime)
    val endReason = node.get("Task End Reason")
    val status = taskStatus(taskInfo, endReason)
    val errorMessage = if (status == "SUCCESS") None else taskErrorMessage(endReason)
    val metrics = node.get("Task Metrics")
    val hasMetrics = metrics != null && metrics.isObject

    Some(new org.apache.spark.status.TaskDataWrapper(
      taskId = java.lang.Long.valueOf(taskId),
      index = index,
      attempt = attempt,
      partitionId = partitionId,
      launchTime = launchTime,
      resultFetchStart = gettingResultTime,
      duration = duration,
      executorId = text(taskInfo, "Executor ID").getOrElse(""),
      host = text(taskInfo, "Host").getOrElse(""),
      status = status,
      taskLocality = text(taskInfo, "Locality").getOrElse("UNKNOWN"),
      speculative = boolean(taskInfo, "Speculative").getOrElse(false),
      accumulatorUpdates = taskAccumulatorUpdates(taskInfo),
      errorMessage = errorMessage,
      hasMetrics = hasMetrics,
      executorDeserializeTime = taskMetric(metrics, status, "Executor Deserialize Time"),
      executorDeserializeCpuTime = taskMetric(metrics, status, "Executor Deserialize CPU Time"),
      executorRunTime = taskMetric(metrics, status, "Executor Run Time"),
      executorCpuTime = taskMetric(metrics, status, "Executor CPU Time"),
      resultSize = taskMetric(metrics, status, "Result Size"),
      jvmGcTime = taskMetric(metrics, status, "JVM GC Time"),
      resultSerializationTime = taskMetric(metrics, status, "Result Serialization Time"),
      memoryBytesSpilled = taskMetric(metrics, status, "Memory Bytes Spilled"),
      diskBytesSpilled = taskMetric(metrics, status, "Disk Bytes Spilled"),
      peakExecutionMemory = taskMetric(metrics, status, "Peak Execution Memory"),
      inputBytesRead = nestedTaskMetric(metrics, status, "Input Metrics", "Bytes Read"),
      inputRecordsRead = nestedTaskMetric(metrics, status, "Input Metrics", "Records Read"),
      outputBytesWritten = nestedTaskMetric(metrics, status, "Output Metrics", "Bytes Written"),
      outputRecordsWritten = nestedTaskMetric(metrics, status, "Output Metrics", "Records Written"),
      shuffleRemoteBlocksFetched = nestedTaskMetric(
        metrics, status, "Shuffle Read Metrics", "Remote Blocks Fetched"),
      shuffleLocalBlocksFetched = nestedTaskMetric(
        metrics, status, "Shuffle Read Metrics", "Local Blocks Fetched"),
      shuffleFetchWaitTime = nestedTaskMetric(
        metrics, status, "Shuffle Read Metrics", "Fetch Wait Time"),
      shuffleRemoteBytesRead = nestedTaskMetric(
        metrics, status, "Shuffle Read Metrics", "Remote Bytes Read"),
      shuffleRemoteBytesReadToDisk = nestedTaskMetric(
        metrics, status, "Shuffle Read Metrics", "Remote Bytes Read To Disk"),
      shuffleLocalBytesRead = nestedTaskMetric(
        metrics, status, "Shuffle Read Metrics", "Local Bytes Read"),
      shuffleRecordsRead = nestedTaskMetric(
        metrics, status, "Shuffle Read Metrics", "Total Records Read"),
      shuffleBytesWritten = nestedTaskMetric(
        metrics, status, "Shuffle Write Metrics", "Shuffle Bytes Written"),
      shuffleWriteTime = nestedTaskMetric(
        metrics, status, "Shuffle Write Metrics", "Shuffle Write Time"),
      shuffleRecordsWritten = nestedTaskMetric(
        metrics, status, "Shuffle Write Metrics", "Shuffle Records Written"),
      stageId = stageId,
      stageAttemptId = stageAttemptId))
  }

  private def taskErrorMessage(endReason: JsonNode): Option[String] = {
    if (endReason == null || !endReason.isObject) return None
    Seq("Full Stack Trace", "Description", "Kill Reason", "Loss Reason", "Message", "Reason")
      .iterator
      .flatMap(field => text(endReason, field))
      .find(_.nonEmpty)
  }

  private def taskStatus(taskInfo: JsonNode, endReason: JsonNode): String = {
    val reason = Option(endReason).flatMap(text(_, "Reason")).getOrElse("")
    if (boolean(taskInfo, "Killed").contains(true) || reason.contains("TaskKilled")) {
      "KILLED"
    } else if (boolean(taskInfo, "Failed").contains(true) ||
        (reason.nonEmpty && reason != "Success")) {
      "FAILED"
    } else if (long(taskInfo, "Finish Time").exists(_ > 0L)) {
      "SUCCESS"
    } else {
      "RUNNING"
    }
  }

  private def taskMetric(metrics: JsonNode, status: String, field: String): Long = {
    val value = if (metrics == null || !metrics.isObject) {
      None
    } else {
      Some(long(metrics, field).getOrElse(0L))
    }
    encodeTaskMetric(value, status)
  }

  private def nestedTaskMetric(
      metrics: JsonNode,
      status: String,
      objectField: String,
      field: String): Long = {
    val value = if (metrics == null || !metrics.isObject) {
      None
    } else {
      val nested = metrics.get(objectField)
      if (nested == null || !nested.isObject) {
        Some(0L)
      } else {
        Some(long(nested, field).getOrElse(0L))
      }
    }
    encodeTaskMetric(value, status)
  }

  private def encodeTaskMetric(value: Option[Long], status: String): Long = {
    value match {
      case Some(metric) if status == "SUCCESS" => metric
      case Some(metric) => -metric - 1L
      case None => -1L
    }
  }

  private def taskMetricValue(
      task: org.apache.spark.status.TaskDataWrapper,
      value: Long): Long = {
    if (!task.hasMetrics) {
      0L
    } else if (task.status == "SUCCESS") {
      math.max(0L, value)
    } else {
      math.abs(value + 1L)
    }
  }

  private def taskAccumulatorUpdates(taskInfo: JsonNode): Seq[AccumulableInfo] = {
    val accumulables = taskInfo.get("Accumulables")
    if (accumulables == null || !accumulables.isArray) {
      Seq.empty
    } else {
      accumulables.elements().asScala.flatMap(toTaskAccumulatorUpdate).toSeq
    }
  }

  private def toTaskAccumulatorUpdate(acc: JsonNode): Option[AccumulableInfo] = {
    if (boolean(acc, "Internal").contains(true) ||
        text(acc, "Metadata").contains(SqlAccumulatorMetadata)) {
      None
    } else {
      long(acc, "ID").map { id =>
        new AccumulableInfo(
          id = id,
          name = text(acc, "Name").orNull,
          update = jsonValue(acc.get("Update")),
          value = jsonValue(acc.get("Value")).orNull)
      }
    }
  }

  private def jsonValue(node: JsonNode): Option[String] = {
    if (node == null || node.isNull) {
      None
    } else if (node.isTextual) {
      Some(node.asText())
    } else {
      Some(node.toString)
    }
  }

  private def sparkPlanInfo(node: JsonNode): Option[SparkPlanInfo] = {
    if (node == null || !node.isObject) {
      None
    } else {
      val children = node.get("children") match {
        case values if values != null && values.isArray =>
          values.elements().asScala.flatMap(sparkPlanInfo).toSeq
        case _ =>
          Seq.empty
      }
      val metrics = node.get("metrics") match {
        case values if values != null && values.isArray =>
          values.elements().asScala.flatMap(sqlMetricInfo).toSeq
        case _ =>
          Seq.empty
      }
      Some(new SparkPlanInfo(
        nodeName = text(node, "nodeName").getOrElse("Unknown"),
        simpleString = text(node, "simpleString").getOrElse(""),
        children = children,
        metadata = objectFields(node.get("metadata")).toMap,
        metrics = metrics))
    }
  }

  private def sqlMetricInfo(node: JsonNode): Option[SQLMetricInfo] = {
    if (node == null || !node.isObject) {
      None
    } else {
      long(node, "accumulatorId").map { accumulatorId =>
        new SQLMetricInfo(
          name = text(node, "name").getOrElse(""),
          accumulatorId = accumulatorId,
          metricType = text(node, "metricType").getOrElse("sum"))
      }
    }
  }

  private def sqlPlanMetric(node: JsonNode): Option[SQLPlanMetric] = {
    if (node == null || !node.isObject) {
      None
    } else {
      long(node, "accumulatorId").map { accumulatorId =>
        SQLPlanMetric(
          name = text(node, "name").getOrElse(""),
          accumulatorId = accumulatorId,
          metricType = text(node, "metricType").getOrElse("sum"))
      }
    }
  }

  private def mergeSqlMetrics(
      existing: Seq[SQLPlanMetric],
      updates: Seq[SQLPlanMetric]): Seq[SQLPlanMetric] = {
    (existing ++ updates)
      .groupBy(_.accumulatorId)
      .toSeq
      .sortBy(_._1)
      .map(_._2.last)
  }

  private def formatSqlMetricValue(
      metricType: String,
      values: Array[Long],
      maxMetric: Array[Long]): String = {
    try {
      SQLMetrics.stringValue(metricType, values, maxMetric)
    } catch {
      case NonFatal(_) =>
        values.sum.toString
    }
  }

  private def handleSqlTaskMetrics(
      node: JsonNode,
      stageId: Int,
      attemptId: Int,
      state: SummaryState): Unit = {
    val taskInfo = node.get("Task Info")
    if (taskInfo == null || !taskInfo.isObject) return

    val updates = sqlAccumulatorUpdates(taskInfo)
    if (updates.isEmpty) return

    val taskId = long(taskInfo, "Task ID").getOrElse(return)
    val index = int(taskInfo, "Index").getOrElse(return)
    val status = taskStatus(taskInfo, node.get("Task End Reason"))
    state.bufferSqlTaskMetrics(
      taskId,
      StageKey(stageId, attemptId),
      index,
      succeeded = status == "SUCCESS",
      updates)
  }

  private def sqlAccumulatorUpdates(taskInfo: JsonNode): Seq[(Long, Long)] = {
    val accumulables = taskInfo.get("Accumulables")
    if (accumulables == null || !accumulables.isArray) {
      Seq.empty
    } else {
      accumulables.elements().asScala.flatMap { acc =>
        if (text(acc, "Metadata").contains(SqlAccumulatorMetadata)) {
          for {
            id <- long(acc, "ID")
            value <- long(acc, "Update").orElse(long(acc, "Value"))
          } yield id -> value
        } else {
          None
        }
      }.toSeq
    }
  }

  private def toStoredSqlNodes(nodes: Seq[SparkPlanGraphNode]): Seq[SparkPlanGraphNodeWrapper] = {
    nodes.map {
      case cluster: SparkPlanGraphCluster =>
        val storedCluster = new SparkPlanGraphClusterWrapper(
          cluster.id,
          cluster.name,
          cluster.desc,
          toStoredSqlNodes(cluster.nodes.toSeq),
          cluster.metrics)
        new SparkPlanGraphNodeWrapper(null, storedCluster)
      case node =>
        new SparkPlanGraphNodeWrapper(node, null)
    }
  }

  private def toRDDOperationGraph(stage: StageSummary): org.apache.spark.status.RDDOperationGraphWrapper = {
    val nodes = mutable.HashMap.empty[Int, RDDOperationNode]
    val clusterData = mutable.HashMap.empty[String, RddClusterSummary]
    val rootCluster = RddClusterSummary(
      id = s"stage_${stage.key.stageId}",
      name = s"Stage ${stage.key.stageId}" +
        (if (stage.key.attemptId == 0) "" else s" (attempt ${stage.key.attemptId})"))
    val edges = mutable.ArrayBuffer.empty[RDDOperationEdge]

    stage.rddInfos.sortBy(_.id).foreach { rdd =>
      val node = nodes.getOrElseUpdate(rdd.id, RDDOperationNode(
        rdd.id,
        rdd.name,
        rdd.cached,
        rdd.barrier,
        rdd.callsite,
        rdd.deterministicLevel))
      edges ++= rdd.parentIds.map(RDDOperationEdge(_, rdd.id))

      rdd.scope match {
        case Some(scope) =>
          val clusters = scope.allScopes.map { item =>
            clusterData.getOrElseUpdate(item.id, RddClusterSummary(item.id, item.name))
          }
          clusters.sliding(2).foreach {
            case Seq(parent, child) => parent.attachChildCluster(child)
            case _ =>
          }
          clusters.headOption.foreach(rootCluster.attachChildCluster)
          clusters.lastOption.foreach(_.attachChildNode(node))
        case None =>
          rootCluster.attachChildNode(node)
      }
    }

    val knownNodes = nodes.keySet
    val internalEdges = mutable.ArrayBuffer.empty[RDDOperationEdge]
    val outgoingEdges = mutable.ArrayBuffer.empty[RDDOperationEdge]
    val incomingEdges = mutable.ArrayBuffer.empty[RDDOperationEdge]
    edges.foreach { edge =>
      val fromThisGraph = knownNodes.contains(edge.fromId)
      val toThisGraph = knownNodes.contains(edge.toId)
      if (fromThisGraph && toThisGraph) {
        internalEdges += edge
      } else if (fromThisGraph) {
        outgoingEdges += edge
      } else if (toThisGraph) {
        incomingEdges += edge
      }
    }

    new org.apache.spark.status.RDDOperationGraphWrapper(
      stage.key.stageId,
      internalEdges.toSeq,
      outgoingEdges.toSeq,
      incomingEdges.toSeq,
      rootCluster.toWrapper)
  }

  private def writeSummaryShards(
      writer: UIMetaV2Writer,
      summary: Summary,
      attemptId: Option[String]): Unit = {
    val appRecords: Seq[(String, AnyRef)] = Seq(
      classOf[org.apache.spark.status.ApplicationInfoWrapper].getName ->
        new org.apache.spark.status.ApplicationInfoWrapper(toApplicationInfo(summary, attemptId)),
      classOf[org.apache.spark.status.ApplicationEnvironmentInfoWrapper].getName ->
        new org.apache.spark.status.ApplicationEnvironmentInfoWrapper(
          toEnvironmentInfo(summary.environment)),
      classOf[org.apache.spark.status.AppSummary].getName ->
        new org.apache.spark.status.AppSummary(
          summary.jobs.count(_.status == JobExecutionStatus.SUCCEEDED),
          summary.stages.count(_.status == StageStatus.COMPLETE)))
    writer.writeShard("app-00000", "app", None, None, appRecords)

    val stageRecords = summary.stages
      .sortBy(s => (s.key.stageId, s.key.attemptId))
      .iterator
      .flatMap { stage =>
        val baseRecords = Iterator[(String, AnyRef)](
          classOf[org.apache.spark.status.StageDataWrapper].getName ->
            new org.apache.spark.status.StageDataWrapper(
              toStageData(stage),
              stage.jobIds.toSet,
              stage.localitySummary.toMap),
          classOf[org.apache.spark.status.RDDOperationGraphWrapper].getName ->
            toRDDOperationGraph(stage)
        )
        baseRecords ++ stage.executorSummaries.valuesIterator.map { summary =>
          classOf[ExecutorStageSummaryWrapper].getName -> summary.toWrapper(
            stage.key.stageId,
            stage.key.attemptId)
        }
      }
    writeSummaryRecordShards(writer, "stages", "stages", stageRecords)

    val stageMap = summary.stages.map(stage => stage.key -> stage).toMap
    val jobRecords = summary.jobs.sortBy(_.jobId).iterator.map { job =>
      classOf[org.apache.spark.status.JobDataWrapper].getName ->
        new org.apache.spark.status.JobDataWrapper(
          toJobData(job, summary, stageMap),
          Set.empty,
          job.sqlExecutionId)
    }
    writeSummaryRecordShards(writer, "jobs", "jobs", jobRecords)

    val sqlRecords = summary.sqlExecutions
      .sortBy(_.executionId)
      .iterator
      .flatMap(sqlRecordsFor(summary, _))
    if (sqlRecords.nonEmpty) {
      writeSummaryRecordShards(writer, "sql", "sql", sqlRecords)
    }
  }

  private def writeSummaryRecordShards(
      writer: UIMetaV2Writer,
      idPrefix: String,
      kind: String,
      records: Iterator[(String, AnyRef)]): Unit = {
    val buffer = mutable.ArrayBuffer.empty[(String, AnyRef)]
    var shardIndex = 0

    def flush(): Unit = {
      if (buffer.nonEmpty) {
        writer.writeShard(f"$idPrefix-$shardIndex%05d", kind, None, None, buffer.toSeq)
        buffer.clear()
        shardIndex += 1
      }
    }

    records.foreach { record =>
      buffer += record
      if (buffer.size >= SummaryShardRecords) {
        flush()
      }
    }
    flush()
  }

  private def sqlRecordsFor(
      summary: Summary,
      execution: SQLExecutionSummary): Seq[(String, AnyRef)] = {
    execution.graph.toSeq.flatMap { graph =>
      val jobs = summary.jobs
        .filter(_.sqlExecutionId.contains(execution.executionId))
      val jobStatuses = jobs.map(job => job.jobId -> job.status).toMap
      val stages = jobs.flatMap(_.stageIds).toSet
      Seq[(String, AnyRef)](
        classOf[SQLExecutionUIData].getName -> new SQLExecutionUIData(
          executionId = execution.executionId,
          description = execution.description,
          details = execution.details,
          physicalPlanDescription = execution.physicalPlanDescription,
          modifiedConfigs = execution.modifiedConfigs,
          metrics = execution.metrics,
          submissionTime = execution.submissionTime,
          completionTime = execution.completionTime,
          jobs = jobStatuses,
          stages = stages,
          metricValues = execution.metricValues),
        classOf[SparkPlanGraphWrapper].getName -> graph)
    }
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
      memoryPerExecutorMB = summary.environment.sparkProperties.get("spark.executor.memory")
        .flatMap(parseMemoryMb),
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

  private def toJobData(
      job: JobSummary,
      summary: Summary,
      stageMap: Map[StageKey, StageSummary]): JobData = {
    val stages = job.stageKeys.flatMap(stageMap.get)
    val numTasks = stages.map(_.numTasks).sum
    val completedStages = stages.count(_.status == StageStatus.COMPLETE)
    val failedStages = stages.count(_.status == StageStatus.FAILED)
    val skippedStages = if (job.status == JobExecutionStatus.SUCCEEDED) {
      math.max(0, job.stageIds.size - completedStages)
    } else {
      0
    }
    val completedTasks = stages.map(_.numCompletedTasks).sum
    val failedTasks = stages.map(_.numFailedTasks).sum
    val killedTasks = stages.map(_.numKilledTasks).sum
    val completedIndices = stages.map(_.numCompletedIndices).sum
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
      numActiveTasks = if (job.status == JobExecutionStatus.RUNNING) {
        math.max(0, numTasks - completedTasks)
      } else {
        0
      },
      numCompletedTasks = completedTasks,
      numSkippedTasks = stages.filter(_.status == StageStatus.SKIPPED).map(_.numTasks).sum,
      numFailedTasks = failedTasks,
      numKilledTasks = killedTasks,
      numCompletedIndices = completedIndices,
      numActiveStages = activeStages,
      numCompletedStages = completedStages,
      numSkippedStages = skippedStages,
      numFailedStages = failedStages,
      killedTasksSummary = mergeKilledTaskSummaries(stages))
  }

  private def mergeKilledTaskSummaries(stages: Seq[StageSummary]): Map[String, Int] = {
    stages
      .flatMap(_.killedTasksSummary)
      .groupBy(_._1)
      .map { case (reason, values) => reason -> values.map(_._2).sum }
  }

  private def toStageData(stage: StageSummary): StageData = {
    val metrics = stage.metrics
    val completedTasks = stage.numCompletedTasks
    val failedTasks = stage.numFailedTasks
    val killedTasks = stage.numKilledTasks
    val activeTasks = if (stage.status == StageStatus.ACTIVE) stage.numTasks else 0
    new StageData(
      status = stage.status,
      stageId = stage.key.stageId,
      attemptId = stage.key.attemptId,
      numTasks = stage.numTasks,
      numActiveTasks = activeTasks,
      numCompleteTasks = completedTasks,
      numFailedTasks = failedTasks,
      numKilledTasks = killedTasks,
      numCompletedIndices = stage.numCompletedIndices,
      submissionTime = stage.submissionTime.map(new Date(_)),
      firstTaskLaunchedTime = stage.firstTaskLaunchedTime.map(new Date(_)),
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
      description = stage.description,
      details = stage.details,
      schedulingPool = stage.schedulingPool,
      rddIds = stage.rddIds,
      accumulatorUpdates = stage.accumulatorUpdates,
      tasks = None,
      executorSummary = None,
      speculationSummary = None,
      killedTasksSummary = stage.killedTasksSummary.toMap,
      resourceProfileId = stage.resourceProfileId,
      peakExecutorMetrics = None,
      taskMetricsDistributions = None,
      executorMetricsDistributions = None)
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

  private def boolean(node: JsonNode, field: String): Option[Boolean] = {
    if (node == null) return None
    val value = node.get(field)
    if (value == null || value.isNull) None
    else if (value.isBoolean) Some(value.asBoolean())
    else parseBoolean(value.asText())
  }

  private def parseBoolean(value: String): Option[Boolean] = {
    value.toLowerCase(java.util.Locale.ROOT) match {
      case "true" => Some(true)
      case "false" => Some(false)
      case _ => None
    }
  }

  private def parseLong(value: String): Option[Long] = {
    try {
      Some(value.toLong)
    } catch {
      case _: NumberFormatException => None
    }
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

  private def tupleLongLongArray(node: JsonNode): Seq[(Long, Long)] = {
    if (node == null || !node.isArray) {
      Seq.empty
    } else {
      node.elements().asScala.flatMap { item =>
        if (item.isArray && item.size() >= 2) {
          for {
            id <- longValue(item.get(0))
            value <- longValue(item.get(1))
          } yield id -> value
        } else if (item.isObject) {
          for {
            id <- long(item, "_1").orElse(long(item, "id")).orElse(long(item, "accumulatorId"))
            value <- long(item, "_2").orElse(long(item, "value"))
          } yield id -> value
        } else {
          None
        }
      }.toSeq
    }
  }

  private def longValue(node: JsonNode): Option[Long] = {
    if (node == null || node.isNull) None
    else if (node.isNumber) Some(node.asLong())
    else parseLong(node.asText())
  }

  private def rddInfos(node: JsonNode): Seq[RddInfoSummary] = {
    if (node == null || !node.isArray) {
      Seq.empty
    } else {
      node.elements().asScala.flatMap { rdd =>
        int(rdd, "RDD ID").map { id =>
          RddInfoSummary(
            id = id,
            name = text(rdd, "Name").getOrElse(s"RDD $id"),
            parentIds = intArray(rdd.get("Parent IDs")),
            scope = text(rdd, "Scope").flatMap(rddScope),
            callsite = text(rdd, "Callsite").getOrElse(""),
            cached = isCached(rdd.get("Storage Level")),
            barrier = boolean(rdd, "Barrier").getOrElse(false),
            deterministicLevel = deterministicLevel(rdd))
        }
      }.toSeq
    }
  }

  private def rddScope(value: String): Option[RddScopeSummary] = {
    try {
      rddScope(mapper.readTree(value))
    } catch {
      case NonFatal(_) => None
    }
  }

  private def rddScope(node: JsonNode): Option[RddScopeSummary] = {
    if (node == null || !node.isObject) {
      None
    } else {
      for {
        id <- text(node, "id")
        name <- text(node, "name")
      } yield RddScopeSummary(id, name, rddScope(node.get("parent")))
    }
  }

  private def isCached(storageLevel: JsonNode): Boolean = {
    boolean(storageLevel, "Use Memory").contains(true) ||
      boolean(storageLevel, "Use Disk").contains(true)
  }

  private def deterministicLevel(rdd: JsonNode): DeterministicLevel.Value = {
    text(rdd, "DeterministicLevel").getOrElse("DETERMINATE") match {
      case "INDETERMINATE" => DeterministicLevel.INDETERMINATE
      case "UNORDERED" => DeterministicLevel.UNORDERED
      case _ => DeterministicLevel.DETERMINATE
    }
  }

  private def inferAppId(path: Path): String = {
    path.getName.stripSuffix(".inprogress")
  }

  private def isInProgressPath(path: Path): Boolean = {
    path.getName.endsWith(".inprogress")
  }

  private case class ParsedEventLog(
      summary: Summary,
      writerOpt: Option[UIMetaV2Writer],
      totalTasks: Long,
      newWriter: (String, Option[String], Boolean) => UIMetaV2Writer) {

    def writerFor(summary: Summary, attemptId: Option[String]): UIMetaV2Writer = {
      writerOpt.getOrElse(newWriter(summary.appId, attemptId, summary.completed))
    }
  }

  private class ParseContext(
      val state: SummaryState,
      eventLog: String,
      uimetaDir: String,
      attemptIdOverride: Option[String],
      compression: String,
      taskShardRecords: Int,
      hadoopConf: Configuration) {
    private var writer: Option[UIMetaV2Writer] = None
    val taskBuffer = new TaskShardBuffer(() => ensureWriter(), taskShardRecords)

    def writerOpt: Option[UIMetaV2Writer] = writer

    def totalTasks: Long = taskBuffer.totalTasks

    def flushTasks(): Unit = taskBuffer.flushAll()

    def newWriter(
        appId: String,
        attemptId: Option[String],
        completed: Boolean): UIMetaV2Writer = {
      new UIMetaV2Writer(
        logDir = uimetaDir,
        appId = appId,
        attemptId = attemptId,
        completed = completed,
        sparkVersion = org.apache.spark.SPARK_VERSION,
        sourceEventLog = eventLog,
        compression = compression,
        hadoopConf = hadoopConf)
    }

    private def ensureWriter(): UIMetaV2Writer = {
      writer.getOrElse {
        val attemptId = attemptIdOverride.orElse(state.attemptId)
        if (!state.authoritativeAppIdSeen) {
          throw new IllegalStateException(
            "Cannot write task shards before application metadata is available")
        }
        val created = newWriter(state.currentAppId, attemptId, completed = false)
        state.pathMetadataFrozen = true
        writer = Some(created)
        created
      }
    }
  }

  private case class StageAttemptKey(stageId: Int, attemptId: Int)

  private class TaskShardBuffer(
      writer: () => UIMetaV2Writer,
      maxRecords: Int) {
    private val buffers = mutable.HashMap.empty[
      StageAttemptKey,
      mutable.ArrayBuffer[org.apache.spark.status.TaskDataWrapper]]
    private val shardIndexes = mutable.HashMap.empty[StageAttemptKey, Int]
    var totalTasks: Long = 0L

    def append(
        key: StageAttemptKey,
        task: org.apache.spark.status.TaskDataWrapper): Unit = {
      val buffer = buffers.getOrElseUpdate(
        key,
        mutable.ArrayBuffer.empty[org.apache.spark.status.TaskDataWrapper])
      buffer += task
      totalTasks += 1L
      if (buffer.size >= maxRecords) {
        flush(key)
      }
    }

    def flush(key: StageAttemptKey): Unit = {
      buffers.get(key).foreach { buffer =>
        if (buffer.nonEmpty) {
          val shardIndex = shardIndexes.getOrElse(key, 0)
          val id = f"tasks-stage-${key.stageId}%06d-attempt-${key.attemptId}%06d-$shardIndex%05d"
          writer().writeShard(
            id = id,
            kind = "tasks",
            stageId = Some(key.stageId),
            stageAttemptId = Some(key.attemptId),
            records = buffer.toSeq.map { task =>
              classOf[org.apache.spark.status.TaskDataWrapper].getName -> task
            })
          buffer.clear()
          shardIndexes.update(key, shardIndex + 1)
        }
      }
    }

    def flushAll(): Unit = {
      val remainingKeys = buffers.keys.toSeq
      remainingKeys.foreach(flush)
      buffers.clear()
    }
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
    var authoritativeAppIdSeen: Boolean = false
    var pathMetadataFrozen: Boolean = false
    val jobs: mutable.LinkedHashMap[Int, JobSummary] = mutable.LinkedHashMap.empty
    val stages: mutable.LinkedHashMap[StageKey, StageSummary] = mutable.LinkedHashMap.empty
    val sqlExecutions: mutable.LinkedHashMap[Long, SQLExecutionSummary] =
      mutable.LinkedHashMap.empty
    private val sqlStageExecutions =
      mutable.HashMap.empty[StageKey, mutable.Set[Long]]
    private val sqlStageMetrics =
      mutable.HashMap.empty[SqlStageMetricKey, SqlStageMetricSummary]
    private var sqlTaskMetricCount = 0L
    var sqlRetainedExecutions: Int = DefaultSqlRetainedExecutions

    def currentAppId: String = appId.getOrElse(defaultAppId)

    def getOrCreateSqlExecution(executionId: Long): SQLExecutionSummary = {
      sqlExecutions.getOrElseUpdate(executionId, SQLExecutionSummary(executionId))
    }

    def registerSqlJob(job: JobSummary): Unit = {
      job.sqlExecutionId.foreach { executionId =>
        val execution = getOrCreateSqlExecution(executionId)
        execution.jobs += job.jobId -> job.status
        execution.stages ++= job.stageIds
        job.stageKeys.foreach { key =>
          sqlStageExecutions.getOrElseUpdate(key, mutable.Set.empty) += executionId
        }
      }
    }

    def bufferSqlTaskMetrics(
        taskId: Long,
        key: StageKey,
        taskIndex: Int,
        succeeded: Boolean,
        updates: Seq[(Long, Long)]): Unit = {
      sqlTaskMetricCount += 1L
      val executionIds = sqlStageExecutions.getOrElse(key, mutable.Set.empty[Long])
      if (executionIds.nonEmpty && updates.nonEmpty) {
        executionIds.foreach { executionId =>
          val metricKey = SqlStageMetricKey(executionId, key)
          val metrics = sqlStageMetrics.getOrElseUpdate(
            metricKey,
            new SqlStageMetricSummary(
              key,
              stages.get(key).map(_.numTasks).getOrElse(0),
              Map.empty))
          metrics.recordMetric(taskId, taskIndex, succeeded, updates)
          if (sqlTaskMetricCount % 100000 == 0) {
            logInfo(s"[stream-v2] Buffered $sqlTaskMetricCount SQL task metrics, " +
              s"sqlStageMetrics size=${sqlStageMetrics.size}")
          }
        }
      }
    }

    def recordTaskEnd(
        key: StageKey,
        task: org.apache.spark.status.TaskDataWrapper): Unit = {
      stages.getOrElseUpdate(key, StageSummary(key)).recordTaskEnd(task)
    }

    def pruneSqlExecutions(): Unit = {
      val maxExecutions = math.max(1, sqlRetainedExecutions)
      while (sqlExecutions.size > maxExecutions) {
        val candidate = sqlExecutions.collectFirst {
          case (executionId, execution) if execution.completionTime.isDefined => executionId
        }.orElse(sqlExecutions.headOption.map(_._1))
        candidate match {
          case Some(executionId) =>
            sqlExecutions.remove(executionId)
            sqlStageMetrics.keys
              .filter(_.executionId == executionId)
              .toSeq
              .foreach(sqlStageMetrics.remove)
            sqlStageExecutions.values.foreach(_ -= executionId)
          case None =>
            return
        }
      }
    }

    def toSummary(): Summary = {
      finalizeSucceedingJobs()
      finalizeSqlMetrics()
      val finalAppId = currentAppId
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
        sqlExecutions = sqlExecutions.values.toSeq,
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

    private def finalizeSqlMetrics(): Unit = {
      logInfo(s"[stream-v2] Starting SQL metrics finalization: ${sqlStageMetrics.size} " +
        s"stage-metric entries, ${sqlStageExecutions.size} stage-execution mappings")
      val startTime = System.currentTimeMillis()
      sqlExecutions.values.foreach { execution =>
        if (execution.metrics.nonEmpty) {
          val accumTypes = execution.metrics.map { metric =>
            metric.accumulatorId -> metric.metricType
          }.toMap
          val relevantMetrics = sqlStageMetrics.iterator.collect {
            case (metricKey, metrics) if metricKey.executionId == execution.executionId => metrics
          }.toSeq
          logInfo(s"[stream-v2] Aggregating metrics for SQL execution ${execution.executionId}: " +
            s"${relevantMetrics.size} stages, ${accumTypes.size} accumulator types")
          if (relevantMetrics.nonEmpty) {
            execution.metricValues = aggregateSqlMetricValuesFromStages(relevantMetrics, accumTypes)
          }
          execution.driverAccumUpdates.foreach { case (id, value) =>
            if (accumTypes.contains(id)) {
              val current = execution.metricValues.getOrElse(id, "0")
              execution.metricValues = execution.metricValues.updated(
                id,
                try {
                  (current.toLong + value).toString
                } catch {
                  case _: Exception => current
                })
            }
          }
        }
      }
      val elapsed = System.currentTimeMillis() - startTime
      logInfo(s"[stream-v2] SQL metrics finalization completed in ${elapsed}ms")
    }

    private def aggregateSqlMetricValuesFromStages(
        stageMetrics: Seq[SqlStageMetricSummary],
        accumTypes: Map[Long, String]): Map[Long, String] = {
      val allMetrics = mutable.HashMap.empty[Long, mutable.ArrayBuilder[Long]]
      val maxMetrics = mutable.HashMap.empty[Long, Array[Long]]

      stageMetrics.foreach { metrics =>
        metrics.metricValues().foreach { case (id, values) =>
          if (accumTypes.contains(id)) {
            val builder = allMetrics.getOrElseUpdate(id, mutable.ArrayBuilder.make[Long])
            var i = 0
            while (i < values.length) {
              if (values(i) != 0L) builder += values(i)
              i += 1
            }
          }
        }
        metrics.maxMetricValues().foreach { case (id, values) =>
          if (accumTypes.contains(id) && SQLMetrics.metricNeedsMax(accumTypes(id))) {
            val current = maxMetrics.get(id)
            if (current.forall(existing => values(0) > existing(0))) {
              maxMetrics.update(id, values.clone())
            }
          }
        }
      }

      allMetrics.map { case (id, builder) =>
        val values = builder.result()
        val maxArray = maxMetrics.getOrElse(id, Array.empty[Long])
        id -> formatSqlMetricValue(accumTypes.getOrElse(id, "sum"), values, maxArray)
      }.toMap
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
      sqlExecutions: Seq[SQLExecutionSummary],
      skippedTaskEvents: Long)

  private case class StageKey(stageId: Int, attemptId: Int)

  private case class SqlStageMetricKey(executionId: Long, stageKey: StageKey)

  private case class RddInfoSummary(
      id: Int,
      name: String,
      parentIds: Seq[Int],
      scope: Option[RddScopeSummary],
      callsite: String,
      cached: Boolean,
      barrier: Boolean,
      deterministicLevel: DeterministicLevel.Value)

  private case class RddScopeSummary(
      id: String,
      name: String,
      parent: Option[RddScopeSummary]) {
    def allScopes: Seq[RddScopeSummary] = {
      parent.map(_.allScopes).getOrElse(Seq.empty) :+ this
    }
  }

  private case class RddClusterSummary(id: String, name: String) {
    private val childNodes = mutable.ArrayBuffer.empty[RDDOperationNode]
    private val childClusters = mutable.LinkedHashMap.empty[String, RddClusterSummary]

    def attachChildNode(node: RDDOperationNode): Unit = {
      if (!childNodes.exists(_.id == node.id)) {
        childNodes += node
      }
    }

    def attachChildCluster(cluster: RddClusterSummary): Unit = {
      childClusters.getOrElseUpdate(cluster.id, cluster)
    }

    def toWrapper: RDDOperationClusterWrapper = {
      new RDDOperationClusterWrapper(
        id,
        name,
        childNodes.toSeq,
        childClusters.values.toSeq.map(_.toWrapper))
    }
  }

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

  private case class SQLExecutionSummary(executionId: Long) {
    var description: String = ""
    var details: String = ""
    var physicalPlanDescription: String = ""
    var modifiedConfigs: Map[String, String] = Map.empty
    var metrics: Seq[SQLPlanMetric] = Seq.empty
    var submissionTime: Long = -1L
    var completionTime: Option[Date] = None
    var graph: Option[SparkPlanGraphWrapper] = None
    var jobs: Map[Int, JobExecutionStatus] = Map.empty
    var stages: Set[Int] = Set.empty
    var driverAccumUpdates: Seq[(Long, Long)] = Seq.empty
    var metricValues: Map[Long, String] = Map.empty
  }

  private class SqlStageMetricSummary(
      val key: StageKey,
      initialNumTasks: Int,
      initialAccumulatorTypes: Map[Long, String]) {
    private var numTasks: Int = math.max(0, initialNumTasks)
    private var accumulatorTypes: Map[Long, String] = initialAccumulatorTypes
    private val completedIndices = mutable.Set.empty[Int]
    private val taskMetrics = mutable.HashMap.empty[Long, Array[Long]]
    private val maxTaskValues = mutable.HashMap.empty[Long, Array[Long]]
    private var metricsRecorded: Long = 0L

    def recordMetric(
        taskId: Long,
        taskIndex: Int,
        succeeded: Boolean,
        updates: Seq[(Long, Long)]): Unit = {
      metricsRecorded += 1L
      if (taskIndex < 0) return
      if (accumulatorTypes.isEmpty) {
        accumulatorTypes = updates.map { case (id, _) => id -> "sum" }.toMap
      }
      if (taskIndex >= numTasks) {
        numTasks = taskIndex + 1
        taskMetrics.keys.toSeq.foreach { id =>
          val values = taskMetrics(id)
          if (taskIndex >= values.length) {
            taskMetrics.update(id, grow(values, numTasks))
          }
        }
      }
      updates.foreach { case (id, value) =>
        accumulatorTypes = accumulatorTypes.updated(id, accumulatorTypes.getOrElse(id, "sum"))
        val values = taskMetrics.getOrElseUpdate(id, new Array[Long](numTasks))
        if (taskIndex < values.length) {
          values(taskIndex) = value
        }
        val current = maxTaskValues.getOrElseUpdate(
          id,
          Array(value, key.stageId.toLong, key.attemptId.toLong, taskId))
        if (value > current(0)) {
          current(0) = value
          current(1) = key.stageId
          current(2) = key.attemptId
          current(3) = taskId
        }
      }
      if (succeeded) {
        completedIndices += taskIndex
      }
    }

    def metricValues(): Seq[(Long, Array[Long])] = {
      taskMetrics.toSeq.map { case (id, values) => id -> values.clone() }
    }

    def maxMetricValues(): Seq[(Long, Array[Long])] = {
      maxTaskValues.toSeq.map { case (id, values) => id -> values.clone() }
    }

    private def grow(values: Array[Long], size: Int): Array[Long] = {
      if (values.length >= size) values
      else {
        val expanded = new Array[Long](size)
        Array.copy(values, 0, expanded, 0, values.length)
        expanded
      }
    }
  }

  private case class StageSummary(key: StageKey) {
    var name: String = s"Stage ${key.stageId}"
    var numTasks: Int = 0
    var submissionTime: Option[Long] = None
    var completionTime: Option[Long] = None
    var firstTaskLaunchedTime: Option[Long] = None
    var failureReason: Option[String] = None
    var description: Option[String] = None
    var details: String = ""
    var schedulingPool: String = SparkUI.DEFAULT_POOL_NAME
    var parentIds: Seq[Int] = Seq.empty
    var rddIds: Seq[Int] = Seq.empty
    var rddInfos: Seq[RddInfoSummary] = Seq.empty
    var resourceProfileId: Int = 0
    var status: StageStatus = StageStatus.PENDING
    var jobIds: mutable.Set[Int] = mutable.LinkedHashSet.empty
    var completedTasks: Int = 0
    var failedTasks: Int = 0
    var killedTasks: Int = 0
    var taskEndEvents: Int = 0
    var accumulatorUpdates: Seq[AccumulableInfo] = Seq.empty
    val completedIndices: mutable.Set[Int] = mutable.LinkedHashSet.empty
    val localitySummary: mutable.Map[String, Long] = mutable.LinkedHashMap.empty
    val killedTasksSummary: mutable.Map[String, Int] = mutable.LinkedHashMap.empty
    val executorSummaries: mutable.LinkedHashMap[String, ExecutorStageSummarySummary] =
      mutable.LinkedHashMap.empty
    val metrics: StageMetrics = StageMetrics()

    def isTerminal: Boolean = status == StageStatus.COMPLETE ||
      status == StageStatus.FAILED ||
      status == StageStatus.SKIPPED

    def recordTaskEnd(task: org.apache.spark.status.TaskDataWrapper): Unit = {
      taskEndEvents += 1
      firstTaskLaunchedTime = Some(
        firstTaskLaunchedTime.map(math.min(_, task.launchTime)).getOrElse(task.launchTime))
      val locality = Option(task.taskLocality).filter(_.nonEmpty).getOrElse("UNKNOWN")
      localitySummary.update(locality, localitySummary.getOrElse(locality, 0L) + 1L)
      task.status match {
        case "SUCCESS" =>
          completedTasks += 1
          completedIndices += task.index
        case "KILLED" =>
          killedTasks += 1
          task.errorMessage.foreach { reason =>
            killedTasksSummary.update(reason, killedTasksSummary.getOrElse(reason, 0) + 1)
          }
        case _ =>
          failedTasks += 1
      }
      if (task.hasMetrics) {
        metrics.addTask(task)
      }
      executorSummaries
        .getOrElseUpdate(task.executorId, ExecutorStageSummarySummary(task.executorId))
        .recordTaskEnd(task)
    }

    def numCompletedTasks: Int = {
      if (taskEndEvents > 0) {
        completedTasks
      } else if (status == StageStatus.COMPLETE) {
        numTasks
      } else {
        0
      }
    }

    def numFailedTasks: Int = {
      if (taskEndEvents > 0) {
        failedTasks
      } else if (status == StageStatus.FAILED) {
        numTasks
      } else {
        0
      }
    }

    def numKilledTasks: Int = killedTasks

    def numCompletedIndices: Int = {
      if (taskEndEvents > 0) completedIndices.size else numCompletedTasks
    }
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
      var shuffleWriteRecords: Long = 0L) {

    def addTask(task: org.apache.spark.status.TaskDataWrapper): Unit = {
      executorDeserializeTime += taskMetricValue(task, task.executorDeserializeTime)
      executorDeserializeCpuTime += taskMetricValue(task, task.executorDeserializeCpuTime)
      executorRunTime += taskMetricValue(task, task.executorRunTime)
      executorCpuTime += taskMetricValue(task, task.executorCpuTime)
      resultSize += taskMetricValue(task, task.resultSize)
      jvmGcTime += taskMetricValue(task, task.jvmGcTime)
      resultSerializationTime += taskMetricValue(task, task.resultSerializationTime)
      memoryBytesSpilled += taskMetricValue(task, task.memoryBytesSpilled)
      diskBytesSpilled += taskMetricValue(task, task.diskBytesSpilled)
      peakExecutionMemory = math.max(
        peakExecutionMemory,
        taskMetricValue(task, task.peakExecutionMemory))
      inputBytes += taskMetricValue(task, task.inputBytesRead)
      inputRecords += taskMetricValue(task, task.inputRecordsRead)
      outputBytes += taskMetricValue(task, task.outputBytesWritten)
      outputRecords += taskMetricValue(task, task.outputRecordsWritten)
      shuffleRemoteBlocksFetched += taskMetricValue(task, task.shuffleRemoteBlocksFetched)
      shuffleLocalBlocksFetched += taskMetricValue(task, task.shuffleLocalBlocksFetched)
      shuffleFetchWaitTime += taskMetricValue(task, task.shuffleFetchWaitTime)
      shuffleRemoteBytesRead += taskMetricValue(task, task.shuffleRemoteBytesRead)
      shuffleRemoteBytesReadToDisk += taskMetricValue(task, task.shuffleRemoteBytesReadToDisk)
      shuffleLocalBytesRead += taskMetricValue(task, task.shuffleLocalBytesRead)
      shuffleReadRecords += taskMetricValue(task, task.shuffleRecordsRead)
      shuffleWriteBytes += taskMetricValue(task, task.shuffleBytesWritten)
      shuffleWriteTime += taskMetricValue(task, task.shuffleWriteTime)
      shuffleWriteRecords += taskMetricValue(task, task.shuffleRecordsWritten)
    }
  }

  private case class ExecutorStageSummarySummary(executorId: String) {
    var taskTime: Long = 0L
    var failedTasks: Int = 0
    var succeededTasks: Int = 0
    var killedTasks: Int = 0
    val metrics: StageMetrics = StageMetrics()

    def recordTaskEnd(task: org.apache.spark.status.TaskDataWrapper): Unit = {
      taskTime += task.duration
      task.status match {
        case "SUCCESS" => succeededTasks += 1
        case "KILLED" => killedTasks += 1
        case _ => failedTasks += 1
      }
      if (task.hasMetrics) {
        metrics.addTask(task)
      }
    }

    def toWrapper(stageId: Int, attemptId: Int): ExecutorStageSummaryWrapper = {
      new ExecutorStageSummaryWrapper(
        stageId,
        attemptId,
        executorId,
        new ExecutorStageSummary(
          taskTime,
          failedTasks,
          succeededTasks,
          killedTasks,
          metrics.inputBytes,
          metrics.inputRecords,
          metrics.outputBytes,
          metrics.outputRecords,
          metrics.shuffleRemoteBytesRead + metrics.shuffleLocalBytesRead,
          metrics.shuffleReadRecords,
          metrics.shuffleWriteBytes,
          metrics.shuffleWriteRecords,
          metrics.memoryBytesSpilled,
          metrics.diskBytesSpilled,
          false,
          None,
          false))
    }
  }

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
}
