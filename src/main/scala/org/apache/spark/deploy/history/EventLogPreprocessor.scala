package org.apache.spark.deploy.history

import java.io.{BufferedOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => NioPath, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.ui.SQLAppStatusListener
import org.apache.spark.scheduler.{ReplayListenerBus, SparkListener, SparkListenerApplicationStart}
import org.apache.spark.status.{AppStatusListener, ElementTrackingStore}
import org.apache.spark.util.kvstore.InMemoryStore

object EventLogPreprocessor {

  def main(args: Array[String]): Unit = {
    val cli = CliArgs.parse(args)
    run(cli)
  }

  private[history] def run(cli: CliArgs): Unit = {
    val sparkConf = new SparkConf()
    val trackingStore = new ElementTrackingStore(new InMemoryStore(), sparkConf)
    try {
      val appStatusListener = new AppStatusListener(
        trackingStore,
        sparkConf,
        live = false,
        appStatusSource = None,
        lastUpdateTime = Some(Files.getLastModifiedTime(cli.eventLog).toMillis))
      val sqlStatusListener = new SQLAppStatusListener(sparkConf, trackingStore, live = false)
      var replayMetadata: Option[ReplayMetadata] = None
      val appMetadataListener = new SparkListener {
        override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
          replayMetadata = Some(ReplayMetadata(
            appId = event.appId.getOrElse {
              throw new IllegalArgumentException(
                s"No application id found in application-start event for ${cli.eventLog}")
            },
            appAttemptId = event.appAttemptId))
        }
      }
      val replayBus = new ReplayListenerBus()
      replayBus.addListener(appStatusListener)
      replayBus.addListener(sqlStatusListener)
      replayBus.addListener(appMetadataListener)

      val eventLogPath = new Path(cli.eventLog.toUri)
      val hadoopConf = new Configuration()
      val fs = FileSystem.get(eventLogPath.toUri, hadoopConf)

      val replayed = withResource(EventLogFileReader.openEventLog(eventLogPath, fs)) { inputStream =>
        replayBus.replay(inputStream, cli.eventLog.toString)
      }
      require(replayed, s"Failed to replay event log ${cli.eventLog}")

      val metadata = replayMetadata.getOrElse {
        throw new IllegalArgumentException(s"No application metadata found in ${cli.eventLog}")
      }
      val appId = metadata.appId
      val attemptId = metadata.appAttemptId.getOrElse("1")

      Files.createDirectories(cli.uimetaDir)
      val uimetaPath = cli.uimetaDir.resolve(s"${appId}_${attemptId}.uimeta")
      withResource(new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(uimetaPath)))) {
        out =>
          UIMetaSnapshotWriter.write(trackingStore, out)
      }

      val resultJson = jsonObject(
        "appId" -> appId,
        "attemptId" -> attemptId,
        "uimetaPath" -> uimetaPath.toAbsolutePath.toString,
        "eventLogPath" -> cli.eventLog.toAbsolutePath.toString
      )
      val resultDir = Option(cli.resultFile.getParent)
      resultDir.foreach(path => Files.createDirectories(path))
      Files.write(cli.resultFile, resultJson.getBytes(StandardCharsets.UTF_8))
    } finally {
      trackingStore.close(closeParent = false)
    }
  }

  private def jsonObject(fields: (String, String)*): String = {
    fields.map { case (key, value) =>
      "\"" + escapeJson(key) + "\":\"" + escapeJson(value) + "\""
    }.mkString("{", ",", "}")
  }

  private def escapeJson(value: String): String = {
    value.flatMap {
      case '\\' => "\\\\"
      case '"' => "\\\""
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case ch if ch.isControl => f"\\u${ch.toInt}%04x"
      case ch => ch.toString
    }
  }

  private def withResource[T <: AutoCloseable, U](resource: T)(f: T => U): U = {
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }

  private case class ReplayMetadata(appId: String, appAttemptId: Option[String])

  private case class CliArgs(eventLog: NioPath, uimetaDir: NioPath, resultFile: NioPath)

  private object CliArgs {
    def parse(args: Array[String]): CliArgs = {
      val parsed = args.grouped(2).collect {
        case Array(flag, value) if flag.startsWith("--") => flag -> value
      }.toMap

      def required(flag: String): String = {
        parsed.getOrElse(flag, throw new IllegalArgumentException(s"Missing required argument: $flag"))
      }

      require(args.length % 2 == 0, "Arguments must be provided as --flag value pairs")

      CliArgs(
        eventLog = Paths.get(required("--eventlog")),
        uimetaDir = Paths.get(required("--uimeta-dir")),
        resultFile = Paths.get(required("--result-file"))
      )
    }
  }
}
