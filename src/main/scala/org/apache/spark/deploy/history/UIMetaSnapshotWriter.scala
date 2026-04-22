package org.apache.spark.deploy.history

import java.io.DataOutputStream

import org.apache.spark.internal.Logging
import org.apache.spark.status.TaskDataWrapper
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

import scala.collection.mutable
import scala.util.control.NonFatal

object UIMetaSnapshotWriter extends Logging {

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

  def write(store: KVStore, out: DataOutputStream): Unit = {
    UIMetaFile.writeHeader(out)

    val writtenHashes = mutable.HashSet.empty[String]
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
      writtenHashes: mutable.Set[String]): Unit = {
    try {
      val iter = store.view(clazz).iterator()
      var count = 0

      while (iter.hasNext) {
        val item = iter.next()
        item match {
          case task: TaskDataWrapper if task.status == "RUNNING" =>
          case _ =>
            try {
              val dataBytes = UIMetaFile.serialize(item.asInstanceOf[AnyRef])
              val hash = java.util.Arrays.hashCode(dataBytes)
              val key = s"${clazz.getName}_$hash"
              if (!writtenHashes.contains(key)) {
                writtenHashes += key
                UIMetaFile.writeRawElement(out, clazz.getName, dataBytes)
                count += 1
              }
            } catch {
              case NonFatal(e) =>
                logWarning(s"Failed to serialize ${clazz.getName} item: ${e.getMessage}")
            }
        }
      }

      if (count > 0) {
        logInfo(s"Dumped $count instances of ${clazz.getSimpleName}")
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to dump ${clazz.getName}: ${e.getMessage}")
    }
  }
}
