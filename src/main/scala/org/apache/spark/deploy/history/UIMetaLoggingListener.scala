package org.apache.spark.deploy.history

import java.io.{BufferedOutputStream, DataOutputStream}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
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
          s"${appId}_${attemptId.getOrElse("1")}.uimeta"
        } else {
          s"${appId}_${attemptId.getOrElse("1")}.uimeta.inprogress"
        }
        val path = new Path(logDir, fileName)
        var tempSt: FSDataOutputStream = null
        var out: DataOutputStream = null
        try {
          if (!fs.exists(new Path(logDir))) fs.mkdirs(new Path(logDir))

          val tempPath = new Path(logDir, s".$fileName.tmp.$snapshotVersion")
          tempSt = fs.create(tempPath, true)
          out = new DataOutputStream(new BufferedOutputStream(tempSt))

          UIMetaSnapshotWriter.write(store, out)

          out.flush()
          out.close(); out = null

          if (fs.exists(path)) fs.delete(path, false)
          fs.rename(tempPath, path)
          snapshotVersion += 1
          logInfo(s"Wrote UIMeta snapshot #$snapshotVersion -> $path")
        } catch {
          case NonFatal(e) => logError("Failed to write UIMeta snapshot", e)
        } finally {
          if (out != null) out.close()
          else if (tempSt != null) tempSt.close()
        }
      case None =>
        logWarning("SparkContext not active, skipping UIMeta dump.")
    }
  }
}
