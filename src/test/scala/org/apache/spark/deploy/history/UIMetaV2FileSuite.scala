package org.apache.spark.deploy.history

import java.io.DataInputStream
import java.nio.file.Files

import org.apache.hadoop.conf.Configuration
import org.apache.spark.status.AppSummary
import org.apache.spark.util.kvstore.InMemoryStore
import org.scalatest.funsuite.AnyFunSuite

class UIMetaV2FileSuite extends AnyFunSuite {

  test("manifest round-trips shard metadata") {
    val manifest = UIMetaV2Manifest(
      formatVersion = 2,
      appId = "local-v2",
      attemptId = Some("1"),
      completed = false,
      sparkVersion = "3.3.0",
      compression = "none",
      createdAt = 1000L,
      sourceEventLog = "file:///tmp/local-v2.inprogress",
      counts = UIMetaV2Counts(jobs = 1, stages = 2, tasks = 3, sqlExecutions = 4),
      shards = Seq(UIMetaV2Shard(
        id = "core-00000",
        kind = "core",
        path = "local-v2_1/core-00000.uimeta.bin",
        classes = Seq(classOf[AppSummary].getName),
        recordCount = 1,
        stageId = None,
        stageAttemptId = None)))

    val json = UIMetaV2Manifest.toJson(manifest)
    val decoded = UIMetaV2Manifest.fromJson(json)
    assert(decoded == manifest)
    assert(decoded.summaryShards.map(_.id) == Seq("core-00000"))
    assert(decoded.taskShardsFor(1, 0).isEmpty)
  }

  test("writer creates a v2 shard that reader loads into kvstore") {
    val tempDir = Files.createTempDirectory("uimeta-v2-suite")
    val hadoopConf = new Configuration()
    val writer = new UIMetaV2Writer(
      logDir = tempDir.toUri.toString,
      appId = "local-v2",
      attemptId = Some("1"),
      completed = true,
      sparkVersion = "3.3.0",
      sourceEventLog = "file:///tmp/local-v2",
      compression = "none",
      hadoopConf = hadoopConf)

    writer.writeShard(
      id = "core-00000",
      kind = "core",
      stageId = None,
      stageAttemptId = None,
      records = Seq(classOf[AppSummary].getName -> new AppSummary(1, 2)))
    val manifestPath = writer.commit()

    val manifest = UIMetaV2Manifest.read(manifestPath, hadoopConf)
    assert(manifest.counts.jobs == 0)
    assert(manifest.shards.head.recordCount == 1)

    val store = new InMemoryStore()
    val reader = new UIMetaV2Reader(tempDir.toUri.toString, hadoopConf)
    reader.loadShards(manifest, manifest.summaryShards, store)

    val summary = store.view(classOf[AppSummary]).iterator()
    assert(summary.hasNext)
    assert(summary.next().numCompletedJobs == 1)
  }

  test("writer and reader support gzip shards") {
    val tempDir = Files.createTempDirectory("uimeta-v2-gzip-suite")
    val hadoopConf = new Configuration()

    val summary = roundTripSummary(
      logDir = tempDir.toUri.toString,
      hadoopConf = hadoopConf,
      compression = "gzip")

    assert(summary.numCompletedJobs == 3)
  }

  test("writer and reader support plain local paths with spaces") {
    val tempBase = Files.createTempDirectory("uimeta-v2-path-suite")
    val tempDir = Files.createDirectories(tempBase.resolve("uimeta with spaces"))
    val hadoopConf = new Configuration()

    val writer = new UIMetaV2Writer(
      logDir = tempDir.toString,
      appId = "local-v2",
      attemptId = Some("1"),
      completed = true,
      sparkVersion = "3.3.0",
      sourceEventLog = "file:///tmp/local-v2",
      compression = "none",
      hadoopConf = hadoopConf)

    writer.writeShard(
      id = "core-00000",
      kind = "core",
      stageId = None,
      stageAttemptId = None,
      records = Seq(classOf[AppSummary].getName -> new AppSummary(3, 4)))
    val manifestPath = writer.commit()

    val manifest = UIMetaV2Manifest.read(manifestPath.toString, hadoopConf)
    val store = new InMemoryStore()
    val reader = new UIMetaV2Reader(tempDir.toString, hadoopConf)
    reader.loadShards(manifest, manifest.summaryShards, store)

    val summary = store.view(classOf[AppSummary]).iterator()
    assert(summary.hasNext)
    assert(summary.next().numCompletedJobs == 3)
  }

  test("UIMetaFile verifies v2 shard header") {
    val temp = Files.createTempFile("uimeta-v2-header", ".bin")
    val out = new java.io.DataOutputStream(new java.io.FileOutputStream(temp.toFile))
    UIMetaFile.writeV2Header(out)
    out.close()

    val in = new DataInputStream(new java.io.FileInputStream(temp.toFile))
    try {
      assert(UIMetaFile.verifyV2Header(in))
    } finally {
      in.close()
    }
  }

  private def roundTripSummary(
      logDir: String,
      hadoopConf: Configuration,
      compression: String): AppSummary = {
    val writer = new UIMetaV2Writer(
      logDir = logDir,
      appId = "local-v2",
      attemptId = Some("1"),
      completed = true,
      sparkVersion = "3.3.0",
      sourceEventLog = "file:///tmp/local-v2",
      compression = compression,
      hadoopConf = hadoopConf)

    writer.writeShard(
      id = "core-00000",
      kind = "core",
      stageId = None,
      stageAttemptId = None,
      records = Seq(classOf[AppSummary].getName -> new AppSummary(3, 4)))
    val manifestPath = writer.commit()
    val manifest = UIMetaV2Manifest.read(manifestPath, hadoopConf)

    val store = new InMemoryStore()
    val reader = new UIMetaV2Reader(logDir, hadoopConf)
    reader.loadShards(manifest, manifest.summaryShards, store)

    val summary = store.view(classOf[AppSummary]).iterator()
    assert(summary.hasNext)
    summary.next()
  }
}
