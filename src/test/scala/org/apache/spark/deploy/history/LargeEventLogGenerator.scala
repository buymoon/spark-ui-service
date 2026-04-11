package org.apache.spark.deploy.history

import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.util.Random

/**
 * Generates a large number of Spark jobs / SQL queries to produce
 * both a traditional event log and a UIMeta snapshot for benchmarking.
 *
 * Target: simulate workloads that would produce ≥1 GB of event log data.
 */
object LargeEventLogGenerator {

  def main(args: Array[String]): Unit = {
    val numJobs   = if (args.length > 0) args(0).toInt else 100
    val tasksPerJob = if (args.length > 1) args(1).toInt else 1000

    val conf = new SparkConf()
      .setAppName("LargeEventLogGenerator")
      .setMaster("local[*]")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "file:///tmp/spark-events")
      .set("spark.extraListeners",
        "org.apache.spark.deploy.history.UIMetaLoggingListener")
      .set("spark.uimeta.dir", "file:///tmp/spark-uimeta")

    new File("/tmp/spark-events").mkdirs()
    new File("/tmp/spark-uimeta").mkdirs()

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    println(s"=== Generating $numJobs jobs × $tasksPerJob tasks ===")

    for (i <- 1 to numJobs) {
      sc.parallelize(1 to tasksPerJob * 100, tasksPerJob)
        .map(_ * Random.nextInt(10))
        .filter(_ % 2 == 0)
        .groupBy(_ % 100)
        .mapValues(_.size)
        .collect()
      if (i % 10 == 0) println(s"  Job $i / $numJobs done")
    }

    // SQL plans
    import spark.implicits._
    for (i <- 1 to 20) {
      sc.parallelize(1 to 100000).toDF("id")
        .createOrReplaceTempView(s"t_$i")
      spark.sql(s"SELECT id, COUNT(*) c FROM t_$i GROUP BY id ORDER BY c DESC LIMIT 10")
        .collect()
    }

    spark.stop()
    println("=== Generation complete. Check /tmp/spark-events & /tmp/spark-uimeta ===")
  }
}
