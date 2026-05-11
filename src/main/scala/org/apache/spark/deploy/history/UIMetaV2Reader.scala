package org.apache.spark.deploy.history

import java.io.{DataInputStream, InputStream}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.KVStore

private[history] class UIMetaV2Reader(logDir: String, hadoopConf: Configuration) extends Logging {

  private val baseDir = new Path(logDir)
  private val fs = baseDir.getFileSystem(hadoopConf)

  def manifestPath(appId: String, attemptId: Option[String], inProgress: Boolean = false): Path = {
    new Path(baseDir, UIMetaV2Manifest.fileName(appId, attemptId, inProgress))
  }

  def findManifest(appId: String, attemptId: Option[String]): Option[Path] = {
    val completed = manifestPath(appId, attemptId, inProgress = false)
    if (fs.exists(completed)) {
      Some(completed)
    } else {
      val inProgress = manifestPath(appId, attemptId, inProgress = true)
      if (fs.exists(inProgress)) Some(inProgress) else None
    }
  }

  def loadShards(manifest: UIMetaV2Manifest, shards: Seq[UIMetaV2Shard], store: KVStore): Unit = {
    val compression = UIMetaV2Compression.normalize(manifest.compression)
    validateCompression(compression)
    shards.foreach { shard =>
      val path = new Path(baseDir, shard.path)
      var rawIn: InputStream = null
      var in: DataInputStream = null
      try {
        rawIn = fs.open(path)
        in = new DataInputStream(inputStream(rawIn, compression))
        rawIn = null
        if (!UIMetaFile.verifyV2Header(in)) {
          throw new IllegalArgumentException(s"Invalid UIMeta v2 shard header in $path")
        }

        var elem = UIMetaFile.readElementBytes(in)
        while (elem.isDefined) {
          val (className, data) = elem.get
          try {
            val clazz = Utils.classForName(className)
            store.write(UIMetaFile.deserialize(data, clazz).asInstanceOf[AnyRef])
          } catch {
            case NonFatal(ex) => logWarning(s"Skip class $className in $path: ${ex.getMessage}")
          }
          elem = UIMetaFile.readElementBytes(in)
        }
      } finally {
        if (in != null) in.close()
        else if (rawIn != null) rawIn.close()
      }
    }
  }

  private def inputStream(in: InputStream, compression: String): InputStream = compression match {
    case "none" | "gzip" | "zstd" =>
      UIMetaV2Compression.inputStream(in, compression, hadoopConf)
    case other => throw new IllegalArgumentException(s"Unsupported UIMeta v2 compression: $other")
  }

  private def validateCompression(compression: String): Unit = {
    UIMetaV2Compression.validate(compression, hadoopConf)
  }
}
