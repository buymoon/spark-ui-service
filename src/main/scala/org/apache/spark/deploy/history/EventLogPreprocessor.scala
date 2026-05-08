package org.apache.spark.deploy.history

import java.io.InputStream

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, FileStatus, Path}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{ReplayListenerBus, SparkListenerInterface}
import org.apache.spark.status.{AppStatusListener, ElementTrackingStore}
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.InMemoryStore

/**
 * Offline converter for existing open-source Spark event logs.
 *
 * It replays the event log through Spark's own listeners, rebuilds the same
 * AppStatusStore used by the History Server, and writes a .uimeta snapshot
 * that UIMetaProvider can load directly.
 */
object EventLogPreprocessor extends Logging {

  private case class Config(
      eventLog: String = "",
      uimetaDir: String = "file:///tmp/spark-uimeta",
      attemptId: Option[String] = None,
      mode: String = "replay")

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)
    val conf = new SparkConf()
      .setAppName("EventLogPreprocessor")
      .set("spark.uimeta.dir", config.uimetaDir)

    val hadoopConf = org.apache.spark.deploy.SparkHadoopUtil.get.newConfiguration(conf)
    val result = config.mode match {
      case "replay" =>
        runReplay(config, conf, hadoopConf)
      case "fast-summary" =>
        FastEventLogSummary.writeUIMeta(
          config.eventLog,
          config.uimetaDir,
          config.attemptId,
          hadoopConf)
      case other =>
        throw new IllegalArgumentException(s"Unknown preprocess mode: $other")
    }

    println(s"eventLog=${result.eventLog}")
    println(s"mode=${result.mode}")
    println(s"appId=${result.appId}")
    println(s"attemptId=${result.attemptId.getOrElse("")}")
    println(s"uimetaPath=${result.uimetaPath}")
    println(s"historyPath=${result.historyPath}")
  }

  private def runReplay(
      config: Config,
      conf: SparkConf,
      hadoopConf: org.apache.hadoop.conf.Configuration): EventLogPreprocessResult = {
    val backingStore = new InMemoryStore()
    val trackingStore = new ElementTrackingStore(backingStore, conf)
    val replayBus = new ReplayListenerBus()

    replayBus.addListener(new AppStatusListener(
      trackingStore,
      conf,
      live = false,
      None,
      None))
    addSqlListener(replayBus, trackingStore, conf)

    val replayed = replayEventLog(config.eventLog, replayBus, hadoopConf)
    if (!replayed) {
      throw new IllegalStateException(s"Failed to replay event log: ${config.eventLog}")
    }

    val appStatusStore = SparkUIServiceCompat.createAppStatusStore(backingStore)
    val appInfo = appStatusStore.applicationInfo()
    val appId = appInfo.id
    val attemptId = config.attemptId.orElse {
      appInfo.attempts.headOption.flatMap(_.attemptId)
    }
    val metaFileName = UIMetaSnapshotWriter.fileName(appId, attemptId)
    val metaPath = UIMetaSnapshotWriter.write(backingStore, config.uimetaDir, metaFileName, hadoopConf)
    val historyPath = attemptId match {
      case Some(id) => s"/history/$appId/$id/jobs/"
      case None => s"/history/$appId/jobs/"
    }

    EventLogPreprocessResult(config.eventLog, appId, attemptId, metaPath, historyPath, "replay")
  }

  private def parseArgs(args: Array[String]): Config = {
    if (args.isEmpty) {
      printUsageAndExit()
    }

    var config = Config()
    val positionals = scala.collection.mutable.ArrayBuffer.empty[String]
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--event-log" =>
          i += 1
          require(i < args.length, "--event-log requires a value")
          config = config.copy(eventLog = args(i))
        case "--uimeta-dir" =>
          i += 1
          require(i < args.length, "--uimeta-dir requires a value")
          config = config.copy(uimetaDir = normalizeUri(args(i)))
        case "--attempt-id" =>
          i += 1
          require(i < args.length, "--attempt-id requires a value")
          config = config.copy(attemptId = Some(args(i)))
        case "--mode" =>
          i += 1
          require(i < args.length, "--mode requires a value")
          config = config.copy(mode = normalizeMode(args(i)))
        case "--fast-summary" =>
          config = config.copy(mode = "fast-summary")
        case "--replay" =>
          config = config.copy(mode = "replay")
        case "--help" | "-h" =>
          printUsageAndExit()
        case value =>
          positionals += value
      }
      i += 1
    }

    if (positionals.nonEmpty && config.eventLog.isEmpty) {
      config = config.copy(eventLog = positionals.head)
    }
    if (positionals.length >= 2 && config.uimetaDir == Config().uimetaDir) {
      config = config.copy(uimetaDir = normalizeUri(positionals(1)))
    }

    require(config.eventLog.nonEmpty, "event log path is required")
    config.copy(eventLog = normalizeUri(config.eventLog))
  }

  private def normalizeMode(value: String): String = {
    value.toLowerCase(java.util.Locale.ROOT) match {
      case "replay" => "replay"
      case "fast-summary" | "fast_summary" | "fast" => "fast-summary"
      case other => throw new IllegalArgumentException(
        s"--mode must be replay or fast-summary, got: $other")
    }
  }

  private def normalizeUri(path: String): String = {
    if (path.contains("://")) path
    else if (path.startsWith("/")) s"file://$path"
    else path
  }

  private def printUsageAndExit(): Nothing = {
    System.err.println(
      """Usage:
        |  EventLogPreprocessor --event-log <path> --uimeta-dir <dir> [--attempt-id <id>] [--mode replay|fast-summary]
        |  EventLogPreprocessor <eventLogPath> [uimetaDir]
        |
        |Modes:
        |  replay        Exact Spark listener replay; slower but complete. This is the default.
        |  fast-summary  Stream JSON lines, skip task events, and write app/job/stage summary metadata.
        |""".stripMargin)
    sys.exit(1)
  }

  private def addSqlListener(
      replayBus: ReplayListenerBus,
      store: ElementTrackingStore,
      conf: SparkConf): Unit = {
    try {
      val clazz = Utils.classForName("org.apache.spark.sql.execution.ui.SQLAppStatusListener")
      val ctor = clazz.getConstructor(classOf[SparkConf], classOf[ElementTrackingStore], java.lang.Boolean.TYPE)
      val listener = ctor.newInstance(conf, store, java.lang.Boolean.FALSE)
        .asInstanceOf[SparkListenerInterface]
      replayBus.addListener(listener)
    } catch {
      case NonFatal(e) =>
        logWarning(s"SQL UI listener is unavailable; SQL UI metadata may be missing: ${e.getMessage}")
    }
  }

  private def replayEventLog(
      eventLog: String,
      replayBus: ReplayListenerBus,
      hadoopConf: org.apache.hadoop.conf.Configuration): Boolean = {
    val path = new Path(eventLog)
    val fs = path.getFileSystem(hadoopConf)

    EventLogFileReader(fs, path) match {
      case Some(reader) =>
        reader.listEventLogFiles.forall(status => replayFile(status, fs, replayBus))
      case None =>
        replayPath(path, fs, replayBus)
    }
  }

  private def replayFile(
      status: FileStatus,
      fs: FileSystem,
      replayBus: ReplayListenerBus): Boolean = {
    replayPath(status.getPath, fs, replayBus)
  }

  private def replayPath(
      path: Path,
      fs: FileSystem,
      replayBus: ReplayListenerBus): Boolean = {
    var in: InputStream = null
    try {
      in = EventLogFileReader.openEventLog(path, fs)
      replayBus.replay(in, path.toString, false)
    } finally {
      if (in != null) in.close()
    }
  }
}
