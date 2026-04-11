package org.apache.spark.deploy.history

import java.io.{DataInputStream, File}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.status.AppStatusStore
import org.apache.spark.util.kvstore.InMemoryStore
import scala.util.control.NonFatal

/**
 * Measures the time taken to load a Spark application UI via:
 *   1. Traditional FsHistoryProvider (event log replay)
 *   2. UIMetaProvider (UIMeta snapshot read)
 *
 * Usage:
 *   spark-submit --class org.apache.spark.deploy.history.ReadPerformanceTest \
 *       spark-uiservice-1.0-SNAPSHOT.jar [appId]
 */
object ReadPerformanceTest {

  def main(args: Array[String]): Unit = {
    val eventsDir = new File("/tmp/spark-events")
    val metaDir   = new File("/tmp/spark-uimeta")

    val appId = if (args.nonEmpty) args(0)
    else {
      val files = eventsDir.listFiles().filter(_.isFile).sortBy(_.lastModified())
      require(files.nonEmpty, "No event logs found in /tmp/spark-events")
      files.last.getName
    }

    println(s"\n===== Performance Test for App: $appId =====\n")

    val conf = new SparkConf()
      .set("spark.history.fs.logDirectory", "file:///tmp/spark-events")
      .set("spark.uimeta.dir", "file:///tmp/spark-uimeta")

    // ---------- Benchmark FsHistoryProvider ----------
    println("[1] FsHistoryProvider (event log replay) ...")
    val fsStart = System.currentTimeMillis()
    val fsProvider = new FsHistoryProvider(conf)
    fsProvider.start()
    // Wait a bit for async listing to complete
    Thread.sleep(5000)
    val fsUI = fsProvider.getAppUI(appId, None)
    val fsEnd = System.currentTimeMillis()
    val fsDur = fsEnd - fsStart
    println(s"    Result: loaded=${fsUI.isDefined}  time=${fsDur} ms")
    fsProvider.stop()

    // ---------- Benchmark UIMetaProvider ----------
    println("[2] UIMetaProvider (UIMeta snapshot) ...")
    val metaStart = System.currentTimeMillis()
    val metaProvider = new UIMetaProvider(conf)
    val metaUI = metaProvider.getAppUI(appId, None)
    val metaEnd = System.currentTimeMillis()
    val metaDur = metaEnd - metaStart
    println(s"    Result: loaded=${metaUI.isDefined}  time=${metaDur} ms")
    metaProvider.stop()

    // ---------- Benchmark raw file sizes ----------
    val eventSize = eventsDir.listFiles()
      .filter(f => f.isFile && f.getName.contains(appId))
      .map(_.length()).sum
    val metaSize = metaDir.listFiles()
      .filter(f => f.isFile && f.getName.contains(appId))
      .map(_.length()).sum

    // ---------- Summary ----------
    println("\n" + "=" * 60)
    println("PERFORMANCE REPORT")
    println("=" * 60)
    println(f"  Event Log Size    : ${eventSize / 1024.0 / 1024.0}%.2f MB")
    println(f"  UIMeta File Size  : ${metaSize  / 1024.0 / 1024.0}%.2f MB")
    if (eventSize > 0) {
      println(f"  Storage Reduction : ${(1.0 - metaSize.toDouble / eventSize) * 100}%.1f%%")
    }
    println()
    println(f"  FsHistoryProvider : $fsDur ms")
    println(f"  UIMetaProvider    : $metaDur ms")
    if (fsDur > 0 && metaDur < fsDur) {
      println(f"  Latency Reduction : ${(1.0 - metaDur.toDouble / fsDur) * 100}%.1f%%")
    }
    println("=" * 60)
  }
}
