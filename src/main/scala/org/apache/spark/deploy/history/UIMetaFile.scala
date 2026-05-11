package org.apache.spark.deploy.history

import java.io.{DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.spark.internal.Logging

/**
 * Utility for reading and writing UIMeta files which store the serialized
 * internal instances of Spark UI models (JobDataWrapper, TaskDataWrapper, etc.)
 *
 * Uses Jackson ObjectMapper for serialization since Spark's wrapper classes
 * are annotated with Jackson annotations (@JsonCreator, @JsonIgnore, etc.)
 *
 * File layout:
 *   [4-byte magic "UI_S"]
 *   repeated: [4-byte class name length] [class name] [4-byte data length] [json data bytes]
 */
object UIMetaFile extends Logging {

  val MAGIC_NUMBER: Array[Byte] = "UI_S".getBytes(StandardCharsets.UTF_8)
  val V2_MAGIC_NUMBER: Array[Byte] = "UI2S".getBytes(StandardCharsets.UTF_8)

  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m
  }

  def writeHeader(out: DataOutputStream): Unit = out.write(MAGIC_NUMBER)

  def writeV2Header(out: DataOutputStream): Unit = out.write(V2_MAGIC_NUMBER)

  def writeElement(out: DataOutputStream, className: String, instance: AnyRef): Unit = {
    val classNameBytes = className.getBytes(StandardCharsets.UTF_8)
    val dataBytes = mapper.writeValueAsBytes(instance)
    out.writeInt(classNameBytes.length)
    out.write(classNameBytes)
    out.writeInt(dataBytes.length)
    out.write(dataBytes)
  }

  def readElementBytes(in: DataInputStream): Option[(String, Array[Byte])] = {
    try {
      val classNameLen = in.readInt()
      val classNameBytes = new Array[Byte](classNameLen)
      in.readFully(classNameBytes)
      val dataLen = in.readInt()
      val dataBytes = new Array[Byte](dataLen)
      in.readFully(dataBytes)
      Some((new String(classNameBytes, StandardCharsets.UTF_8), dataBytes))
    } catch {
      case _: java.io.EOFException => None
    }
  }

  def deserialize[T](dataBytes: Array[Byte], clazz: Class[T]): T = {
    mapper.readValue(dataBytes, clazz)
  }

  def verifyHeader(in: DataInputStream): Boolean = verifyHeaderBytes(in, MAGIC_NUMBER)

  def verifyV2Header(in: DataInputStream): Boolean = verifyHeaderBytes(in, V2_MAGIC_NUMBER)

  private def verifyHeaderBytes(in: DataInputStream, expected: Array[Byte]): Boolean = {
    val magic = new Array[Byte](expected.length)
    try {
      in.readFully(magic)
      magic.sameElements(expected)
    } catch {
      case _: java.io.EOFException => false
    }
  }
}
