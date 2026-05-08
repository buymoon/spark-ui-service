package org.apache.spark.deploy.history

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

import scala.util.control.NonFatal

/**
 * A SparkListener that periodically snapshots the AppStatusStore to a compact
 * UIMeta file using Jackson serialization.
 */
class UIMetaLoggingListener(conf: SparkConf) extends SparkListener with Logging {

  private val logDir = conf.get("spark.uimeta.dir", "/tmp/spark-uimeta")
  private val appId = conf.getOption("spark.app.id")
    .getOrElse(s"local-${System.currentTimeMillis()}")
  private val attemptId = conf.getOption("spark.app.attempt.id")

  private val hadoopConf = new Configuration()
  private val fs = FileSystem.get(new URI(logDir), hadoopConf)

  private var snapshotVersion = 0

  override def onStageCompleted(e: SparkListenerStageCompleted): Unit = writeSnapshot()
  override def onJobEnd(e: SparkListenerJobEnd): Unit = writeSnapshot()
  override def onApplicationEnd(e: SparkListenerApplicationEnd): Unit = writeSnapshot(isFinal = true)

  private def writeSnapshot(isFinal: Boolean = false): Unit = {
    SparkContext.getActive match {
      case Some(sc) =>
        val store = sc.statusStore.store
        val fileName = if (isFinal) {
          UIMetaSnapshotWriter.fileName(appId, attemptId)
        } else {
          UIMetaSnapshotWriter.fileName(appId, attemptId, inProgress = true)
        }
        val path = new Path(logDir, fileName)
        try {
          if (!fs.exists(new Path(logDir))) fs.mkdirs(new Path(logDir))
          UIMetaSnapshotWriter.write(store, logDir, fileName, hadoopConf, snapshotVersion.toString)
          snapshotVersion += 1
          logInfo(s"Wrote UIMeta snapshot #$snapshotVersion -> $path")
        } catch {
          case NonFatal(e) => logError("Failed to write UIMeta snapshot", e)
        }
      case None =>
        logWarning("SparkContext not active, skipping UIMeta dump.")
    }
  }
}
