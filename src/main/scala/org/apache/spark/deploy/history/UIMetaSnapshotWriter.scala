package org.apache.spark.deploy.history

import java.io.{BufferedOutputStream, DataOutputStream}
import java.net.URI

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.status.TaskDataWrapper
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

import scala.collection.mutable
import scala.util.control.NonFatal

private[history] object UIMetaSnapshotWriter extends Logging {

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

  def fileName(appId: String, attemptId: Option[String], inProgress: Boolean = false): String = {
    val suffix = if (inProgress) ".uimeta.inprogress" else ".uimeta"
    s"${appId}_${attemptId.getOrElse("1")}$suffix"
  }

  def write(
      store: KVStore,
      logDir: String,
      targetFileName: String,
      hadoopConf: Configuration = new Configuration(),
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
      dumpStore(store, out)

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

  private def dumpStore(store: KVStore, out: DataOutputStream): Unit = {
    val writtenHashes = new mutable.HashMap[String, Int]()
    coreClasses.foreach(dumpClass(store, _, out, writtenHashes))
    sqlClassNames.foreach { name =>
      try {
        dumpClass(store, Utils.classForName(name), out, writtenHashes)
      } catch {
        case _: ClassNotFoundException =>
      }
    }
  }

  private def dumpClass(
      store: KVStore,
      clazz: Class[_],
      out: DataOutputStream,
      writtenHashes: mutable.HashMap[String, Int]): Unit = {
    try {
      val iter = store.view(clazz).iterator()
      var count = 0
      while (iter.hasNext) {
        val item = iter.next()

        item match {
          case t: TaskDataWrapper if t.status == "RUNNING" =>
          case _ =>
            try {
              val dataBytes = mapper.writeValueAsBytes(item)
              val hash = java.util.Arrays.hashCode(dataBytes)
              val key = s"${clazz.getName}_$hash"
              if (!writtenHashes.contains(key)) {
                writtenHashes.put(key, hash)
                val cn = clazz.getName.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                out.writeInt(cn.length)
                out.write(cn)
                out.writeInt(dataBytes.length)
                out.write(dataBytes)
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
