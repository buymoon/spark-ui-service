package org.apache.spark.deploy.history

import java.io.{BufferedOutputStream, DataOutputStream}
import java.net.URI

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.status.TaskDataWrapper
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

import scala.collection.mutable
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

  private val writtenHashes = new mutable.HashMap[String, Int]()
  private var snapshotVersion = 0

  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m
  }

  private val coreClasses: Seq[Class[_]] = Seq(
    classOf[org.apache.spark.status.JobDataWrapper],
    classOf[org.apache.spark.status.StageDataWrapper],
    classOf[org.apache.spark.status.TaskDataWrapper],
    classOf[org.apache.spark.status.ExecutorStageSummaryWrapper],
    classOf[org.apache.spark.status.ExecutorSummaryWrapper],
    classOf[org.apache.spark.status.ApplicationInfoWrapper],
    classOf[org.apache.spark.status.ApplicationEnvironmentInfoWrapper],
    classOf[org.apache.spark.status.PoolData],
    classOf[org.apache.spark.status.AppSummary],
    classOf[org.apache.spark.status.RDDOperationGraphWrapper]
  )

  private val sqlClassNames = Seq(
    "org.apache.spark.sql.execution.ui.SQLExecutionUIData",
    "org.apache.spark.sql.execution.ui.SparkPlanGraphWrapper"
  )

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

          UIMetaFile.writeHeader(out)

          // Clear dedup map on each full snapshot since we're writing the whole store
          writtenHashes.clear()
          dumpStore(store, out)

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

  private def dumpStore(store: KVStore, out: DataOutputStream): Unit = {
    coreClasses.foreach(dumpClass(store, _, out))
    sqlClassNames.foreach { name =>
      try {
        dumpClass(store, Utils.classForName(name), out)
      } catch { case _: ClassNotFoundException => }
    }
  }

  private def dumpClass(store: KVStore, clazz: Class[_], out: DataOutputStream): Unit = {
    try {
      val iter = store.view(clazz).iterator()
      var count = 0
      while (iter.hasNext) {
        val item = iter.next()

        item match {
          case t: TaskDataWrapper if t.status == "RUNNING" => // skip running tasks
          case _ =>
            try {
              val dataBytes = mapper.writeValueAsBytes(item)
              val hash = java.util.Arrays.hashCode(dataBytes)
              val key = s"${clazz.getName}_$hash"
              if (!writtenHashes.contains(key)) {
                writtenHashes.put(key, hash)
                val cn = clazz.getName.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                out.writeInt(cn.length); out.write(cn)
                out.writeInt(dataBytes.length); out.write(dataBytes)
                count += 1
              }
            } catch {
              case NonFatal(e) =>
                logWarning(s"Failed to serialize ${clazz.getName} item: ${e.getMessage}")
            }
        }
      }
      if (count > 0) logInfo(s"Dumped $count instances of ${clazz.getSimpleName}")
    } catch {
      case NonFatal(e) => logWarning(s"Failed to dump ${clazz.getName}: ${e.getMessage}")
    }
  }
}
