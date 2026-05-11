package org.apache.spark.deploy.history

import java.io.{
  BufferedInputStream,
  BufferedOutputStream,
  ByteArrayInputStream,
  ByteArrayOutputStream,
  DataOutputStream,
  InputStream,
  OutputStream
}
import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.zip.GZIPOutputStream

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

private[history] class UIMetaV2Writer(
    logDir: String,
    appId: String,
    attemptId: Option[String],
    completed: Boolean,
    sparkVersion: String,
    sourceEventLog: String,
    compression: String,
    hadoopConf: Configuration) extends Logging {

  private val baseDir = new Path(logDir)
  private val fs = baseDir.getFileSystem(hadoopConf)
  private val shardDirName = UIMetaV2Manifest.shardDirName(appId, attemptId)
  private val shardDir = new Path(baseDir, shardDirName)
  private val normalizedCompression = UIMetaV2Compression.normalize(compression)
  private val writtenShards = new ArrayBuffer[UIMetaV2Shard]()

  validateCompression()

  def writeShard(
      id: String,
      kind: String,
      stageId: Option[Int],
      stageAttemptId: Option[Int],
      records: Seq[(String, AnyRef)]): UIMetaV2Shard = {
    writeShard(id, kind, stageId, stageAttemptId, records.iterator)
  }

  def writeShard(
      id: String,
      kind: String,
      stageId: Option[Int],
      stageAttemptId: Option[Int],
      records: Iterator[(String, AnyRef)]): UIMetaV2Shard = {
    ensureDirectories()

    val fileName = s"$id.uimeta.bin"
    val finalPath = new Path(shardDir, fileName)
    val tempPath = new Path(shardDir, s".$fileName.tmp.${System.nanoTime()}")
    var rawOut: FSDataOutputStream = null
    var out: DataOutputStream = null
    val classes = new ArrayBuffer[String]()
    var recordCount = 0L

    try {
      rawOut = fs.create(tempPath, true)
      out = new DataOutputStream(outputStream(rawOut))
      UIMetaFile.writeV2Header(out)
      records.foreach { case (className, instance) =>
        UIMetaFile.writeElement(out, className, instance)
        if (!classes.contains(className)) {
          classes += className
        }
        recordCount += 1L
      }
      out.flush()
      out.close()
      out = null
      rawOut = null

      replaceWithTemp(tempPath, finalPath)
      val shard = UIMetaV2Shard(
        id = id,
        kind = kind,
        path = s"$shardDirName/$fileName",
        classes = classes.toSeq,
        recordCount = recordCount,
        stageId = stageId,
        stageAttemptId = stageAttemptId)
      writtenShards += shard
      shard
    } finally {
      if (out != null) out.close()
      else if (rawOut != null) rawOut.close()
      if (fs.exists(tempPath)) fs.delete(tempPath, false)
    }
  }

  def commit(
      counts: UIMetaV2Counts = UIMetaV2Counts(0L, 0L, 0L, 0L),
      completedOverride: Option[Boolean] = None): Path = {
    ensureDirectories()
    val manifestCompleted = completedOverride.getOrElse(completed)

    val manifest = UIMetaV2Manifest(
      formatVersion = UIMetaV2Manifest.FormatVersion,
      appId = appId,
      attemptId = attemptId,
      completed = manifestCompleted,
      sparkVersion = sparkVersion,
      compression = normalizedCompression,
      createdAt = System.currentTimeMillis(),
      sourceEventLog = sourceEventLog,
      counts = counts,
      shards = writtenShards.toSeq)
    val fileName = UIMetaV2Manifest.fileName(appId, attemptId, inProgress = !manifestCompleted)
    val finalPath = new Path(baseDir, fileName)
    val tempPath = new Path(baseDir, s".$fileName.tmp.${System.nanoTime()}")
    var out: FSDataOutputStream = null

    try {
      out = fs.create(tempPath, true)
      out.write(UIMetaV2Manifest.toJson(manifest).getBytes(StandardCharsets.UTF_8))
      out.close()
      out = null
      replaceWithTemp(tempPath, finalPath)
      finalPath
    } finally {
      if (out != null) out.close()
      if (fs.exists(tempPath)) fs.delete(tempPath, false)
    }
  }

  private def ensureDirectories(): Unit = {
    if (!fs.exists(baseDir)) fs.mkdirs(baseDir)
    if (!fs.exists(shardDir)) fs.mkdirs(shardDir)
  }

  private def validateCompression(): Unit = {
    UIMetaV2Compression.validate(normalizedCompression, hadoopConf)
  }

  private def outputStream(out: OutputStream): OutputStream = {
    UIMetaV2Compression.outputStream(out, normalizedCompression, hadoopConf)
  }

  private def replaceWithTemp(tempPath: Path, finalPath: Path): Unit = {
    if (fs.exists(finalPath)) fs.delete(finalPath, false)
    if (!fs.rename(tempPath, finalPath)) {
      throw new IllegalStateException(s"Failed to rename $tempPath to $finalPath")
    }
  }
}

private[history] object UIMetaV2Compression {

  def normalize(compression: String): String = {
    compression.trim.toLowerCase(Locale.ROOT) match {
      case "zstandard" => "zstd"
      case other => other
    }
  }

  def validate(compression: String, hadoopConf: Configuration): Unit = compression match {
    case "none" | "gzip" =>
    case "zstd" =>
      validateZstd(hadoopConf)
    case other => throw new IllegalArgumentException(s"Unsupported UIMeta v2 compression: $other")
  }

  def outputStream(
      out: OutputStream,
      compression: String,
      hadoopConf: Configuration): OutputStream = compression match {
    case "none" => new BufferedOutputStream(out)
    case "gzip" => new GZIPOutputStream(new BufferedOutputStream(out))
    case "zstd" => zstdOutputStream(out, hadoopConf)
    case other => throw new IllegalArgumentException(s"Unsupported UIMeta v2 compression: $other")
  }

  def inputStream(
      in: InputStream,
      compression: String,
      hadoopConf: Configuration): InputStream = compression match {
    case "none" => new BufferedInputStream(in)
    case "gzip" => new java.util.zip.GZIPInputStream(new BufferedInputStream(in))
    case "zstd" => zstdInputStream(in, hadoopConf)
    case other => throw new IllegalArgumentException(s"Unsupported UIMeta v2 compression: $other")
  }

  private def validateZstd(hadoopConf: Configuration): Unit = {
    withZstdError {
      var out: OutputStream = null
      var in: InputStream = null
      try {
        out = zstdOutputStream(new ByteArrayOutputStream(), hadoopConf)
        out.close()
        out = null
        in = zstdInputStream(new ByteArrayInputStream(Array.emptyByteArray), hadoopConf)
        in.close()
        in = null
      } finally {
        if (out != null) out.close()
        if (in != null) in.close()
      }
    }
  }

  private def zstdOutputStream(out: OutputStream, hadoopConf: Configuration): OutputStream = {
    withZstdError {
      zstdCodec(hadoopConf).createOutputStream(new BufferedOutputStream(out))
    }
  }

  private def zstdInputStream(in: InputStream, hadoopConf: Configuration): InputStream = {
    withZstdError {
      zstdCodec(hadoopConf).createInputStream(new BufferedInputStream(in))
    }
  }

  private def withZstdError[T](body: => T): T = {
    try {
      body
    } catch {
      case e: IllegalArgumentException => throw e
      case e: LinkageError =>
        throw new IllegalArgumentException(
          "UIMeta v2 compression zstd is present but could not be initialized in this " +
            "Spark/Hadoop runtime; use --compression gzip or --compression none.",
          e)
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          "UIMeta v2 compression zstd could not be initialized; use --compression gzip " +
            "or --compression none.",
          e)
    }
  }

  def zstdCodec(hadoopConf: Configuration): CompressionCodec = {
    try {
      val clazz = Utils.classForName("org.apache.hadoop.io.compress.ZStandardCodec")
        .asInstanceOf[Class[_ <: CompressionCodec]]
      val codec = clazz.getConstructor().newInstance()
      codec match {
        case configurable: Configurable => configurable.setConf(hadoopConf)
        case _ =>
      }
      codec
    } catch {
      case e: ClassNotFoundException =>
        throw new IllegalArgumentException(
          "UIMeta v2 compression zstd requires Hadoop ZStandardCodec on the classpath; " +
            "use --compression gzip or --compression none if this Spark/Hadoop distribution " +
            "does not provide it.",
          e)
      case e: LinkageError =>
        throw new IllegalArgumentException(
          "UIMeta v2 compression zstd is present but could not be initialized in this " +
            "Spark/Hadoop runtime; use --compression gzip or --compression none.",
          e)
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          "UIMeta v2 compression zstd could not be initialized; use --compression gzip " +
            "or --compression none.",
          e)
    }
  }
}
