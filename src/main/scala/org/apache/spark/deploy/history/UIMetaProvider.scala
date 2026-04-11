package org.apache.spark.deploy.history

import java.io.DataInputStream
import java.net.URI
import java.util.zip.ZipOutputStream

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{ApplicationInfo => ApiApplicationInfo}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.kvstore.InMemoryStore

/**
 * A History Server provider that reads compact UIMeta snapshot files
 * instead of replaying full event logs, providing O(1) UI load time.
 *
 * Falls back to [[FsHistoryProvider]] when a UIMeta file is missing.
 */
class UIMetaProvider(conf: SparkConf) extends ApplicationHistoryProvider with Logging {

  private val logDir = conf.get("spark.uimeta.dir", "/tmp/spark-uimeta")
  private val hadoopConf = org.apache.spark.deploy.SparkHadoopUtil.get.newConfiguration(conf)
  private val fs = FileSystem.get(new URI(logDir), hadoopConf)

  private lazy val fallbackProvider = new FsHistoryProvider(conf)

  override def getListing(): Iterator[ApiApplicationInfo] = {
    // UIMetaProvider skips directory scanning; applications are looked up on demand.
    logInfo("UIMetaProvider.getListing: returning empty to avoid heavy scan.")
    Iterator.empty
  }

  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
    val metaFileName = attemptId match {
      case Some(id) => s"${appId}_$id.uimeta"
      case None     => s"${appId}_1.uimeta"
    }
    val metaPath = new Path(logDir, metaFileName)

    if (fs.exists(metaPath)) {
      try {
        logInfo(s"Loading UIMeta from $metaPath")
        val store = new InMemoryStore()
        val in = new DataInputStream(fs.open(metaPath))
        try {
          if (!UIMetaFile.verifyHeader(in)) {
            logError(s"Invalid header in $metaPath, falling back")
            return fallbackProvider.getAppUI(appId, attemptId)
          }
          var elem = UIMetaFile.readElementBytes(in)
          while (elem.isDefined) {
            val (className, data) = elem.get
            try {
              val clazz = org.apache.spark.util.Utils.classForName(className)
              store.write(UIMetaFile.deserialize(data, clazz))
            } catch {
              case NonFatal(ex) => logWarning(s"Skip class $className: ${ex.getMessage}")
            }
            elem = UIMetaFile.readElementBytes(in)
          }
        } finally {
          in.close()
        }

        val appStatusStore = new AppStatusStore(store)
        val info = appStatusStore.applicationInfo()
        val appName = info.name
        val startTime = info.attempts.headOption
          .map(_.startTime.getTime).getOrElse(System.currentTimeMillis())
        val sparkVersion = info.attempts.headOption
          .flatMap(a => Option(a.appSparkVersion)).getOrElse("3.3.0")

        val secMgr = new SecurityManager(conf)
        val basePath = s"/history/$appId${attemptId.map("/" + _).getOrElse("")}"

        val ui = SparkUI.create(
          None, appStatusStore, conf, secMgr,
          appName, basePath, startTime, sparkVersion
        )

        Some(LoadedAppUI(ui))
      } catch {
        case NonFatal(e) =>
          logError(s"Error loading $metaPath, falling back", e)
          fallbackProvider.getAppUI(appId, attemptId)
      }
    } else {
      logWarning(s"$metaPath not found, falling back to FsHistoryProvider")
      fallbackProvider.getAppUI(appId, attemptId)
    }
  }

  override def getConfig(): Map[String, String] =
    Map("spark.history.provider" -> getClass.getName)

  override def writeEventLogs(
      appId: String,
      attemptId: Option[String],
      zipStream: ZipOutputStream): Unit = {
    // UIMeta doesn't hold raw event logs; delegate to fallback
    fallbackProvider.writeEventLogs(appId, attemptId, zipStream)
  }

  override def stop(): Unit = {
    fallbackProvider.stop()
  }

  override def start(): Unit = {
    // no-op; we don't need to scan directories
  }

  override def getApplicationInfo(appId: String): Option[ApiApplicationInfo] = {
    None // looked up on demand via getAppUI
  }

  override def checkUIViewPermissions(
      appId: String, attemptId: Option[String], user: String): Boolean = true
}
