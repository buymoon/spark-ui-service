package org.apache.spark.deploy.history

import java.io.InputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

private[history] case class UIMetaV2Counts(
    jobs: Long,
    stages: Long,
    tasks: Long,
    sqlExecutions: Long)

private[history] case class UIMetaV2Shard(
    id: String,
    kind: String,
    path: String,
    classes: Seq[String],
    recordCount: Long,
    stageId: Option[Int],
    stageAttemptId: Option[Int])

private[history] case class UIMetaV2Manifest(
    formatVersion: Int,
    appId: String,
    attemptId: Option[String],
    completed: Boolean,
    sparkVersion: String,
    compression: String,
    createdAt: Long,
    sourceEventLog: String,
    counts: UIMetaV2Counts,
    shards: Seq[UIMetaV2Shard]) {

  def appShards: Seq[UIMetaV2Shard] = shardsForKind("app")

  def summaryShards: Seq[UIMetaV2Shard] = shards.filterNot(_.kind == "tasks")

  def shardsForKind(kind: String): Seq[UIMetaV2Shard] = shards.filter(_.kind == kind)

  def taskShardsFor(stageId: Int, stageAttemptId: Int): Seq[UIMetaV2Shard] = {
    shards.filter { shard =>
      shard.kind == "tasks" &&
        shard.stageId.contains(stageId) &&
        shard.stageAttemptId.contains(stageAttemptId)
    }
  }
}

private[history] object UIMetaV2Manifest {

  val FormatVersion: Int = 2

  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m
  }

  def fileName(appId: String, attemptId: Option[String], inProgress: Boolean = false): String = {
    val suffix = if (inProgress) ".uimeta.inprogress.json" else ".uimeta.json"
    s"${appId}_${attemptId.getOrElse("1")}$suffix"
  }

  def shardDirName(appId: String, attemptId: Option[String]): String = {
    s"${appId}_${attemptId.getOrElse("1")}"
  }

  def toJson(manifest: UIMetaV2Manifest): String = mapper.writeValueAsString(manifest)

  def fromJson(json: String): UIMetaV2Manifest = {
    mapper.readValue(json, classOf[UIMetaV2Manifest])
  }

  def read(path: Path, hadoopConf: Configuration): UIMetaV2Manifest = {
    val fs = path.getFileSystem(hadoopConf)
    val in = fs.open(path)
    try {
      mapper.readValue(in.asInstanceOf[InputStream], classOf[UIMetaV2Manifest])
    } finally {
      in.close()
    }
  }

  def read(path: String, hadoopConf: Configuration): UIMetaV2Manifest = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConf)
    val in = fs.open(hadoopPath)
    try {
      mapper.readValue(in.asInstanceOf[InputStream], classOf[UIMetaV2Manifest])
    } finally {
      in.close()
    }
  }
}
