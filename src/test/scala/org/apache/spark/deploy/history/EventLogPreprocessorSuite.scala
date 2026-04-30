package org.apache.spark.deploy.history

import java.io.{DataInputStream, InputStream}
import java.nio.file.Files

import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.ObjectMapper

import org.scalatest.funsuite.AnyFunSuite

class EventLogPreprocessorSuite extends AnyFunSuite {
  private val mapper = new ObjectMapper()

  test("preprocessor preserves replayed attempt ids and captures SQL ui data in uimeta") {
    val fixture = EventLogFixtureBuilder.generate(appAttemptId = Some("attempt-007"))
    val outputDir = Files.createTempDirectory("uimeta-output")
    val resultFile = outputDir.resolve("result.json")

    EventLogPreprocessor.main(Array(
      "--eventlog", fixture.eventLog.toString,
      "--uimeta-dir", outputDir.toString,
      "--result-file", resultFile.toString
    ))

    val expectedAttemptId = fixture.appAttemptId.getOrElse(fail("expected real attempt id in fixture"))
    val expectedUIMeta = outputDir.resolve(s"${fixture.appId}_${expectedAttemptId}.uimeta")
    assert(Files.exists(expectedUIMeta))
    assert(Files.exists(resultFile))

    val result = mapper.readTree(resultFile.toFile)
    assert(result.get("appId").asText() == fixture.appId)
    assert(result.get("attemptId").asText() == expectedAttemptId)
    assert(result.get("uimetaPath").asText() == expectedUIMeta.toAbsolutePath.toString)
    assert(result.get("eventLogPath").asText() == fixture.eventLog.toAbsolutePath.toString)

    val classNames = readUIMetaClassNames(expectedUIMeta)
    assert(classNames.nonEmpty)
    assert(classNames.contains("org.apache.spark.status.ApplicationInfoWrapper"))
    assert(classNames.contains("org.apache.spark.status.JobDataWrapper"))
    assert(classNames.contains("org.apache.spark.status.StageDataWrapper"))
    assert(classNames.contains("org.apache.spark.status.TaskDataWrapper"))
    assert(classNames.exists(_ == "org.apache.spark.sql.execution.ui.SQLExecutionUIData"))
  }

  test("preprocessor falls back to attempt 1 when the event log has no attempt id") {
    val fixture = EventLogFixtureBuilder.generate()
    val outputDir = Files.createTempDirectory("uimeta-output")
    val resultFile = outputDir.resolve("result.json")

    EventLogPreprocessor.main(Array(
      "--eventlog", fixture.eventLog.toString,
      "--uimeta-dir", outputDir.toString,
      "--result-file", resultFile.toString
    ))

    val expectedUIMeta = outputDir.resolve(s"${fixture.appId}_1.uimeta")
    assert(Files.exists(expectedUIMeta))

    val result = mapper.readTree(resultFile.toFile)
    assert(result.get("appId").asText() == fixture.appId)
    assert(result.get("attemptId").asText() == "1")
    assert(result.get("uimetaPath").asText() == expectedUIMeta.toAbsolutePath.toString)
    assert(readUIMetaClassNames(expectedUIMeta).nonEmpty)
  }

  private def readUIMetaClassNames(uimeta: java.nio.file.Path): Seq[String] = {
    val in = new DataInputStream(Files.newInputStream(uimeta))
    try {
      assert(UIMetaFile.verifyHeader(in))
      val classNames = ArrayBuffer.empty[String]
      var next = UIMetaFile.readElementBytes(in)
      while (next.isDefined) {
        classNames += next.get._1
        next = UIMetaFile.readElementBytes(in)
      }
      assert(classNames.nonEmpty)
      classNames.toSeq
    } finally {
      in.close()
    }
  }
}
