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

  private val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m
  }

  def writeHeader(out: DataOutputStream): Unit = out.write(MAGIC_NUMBER)

  def writeElement(out: DataOutputStream, className: String, instance: AnyRef): Unit = {
    writeRawElement(out, className, serialize(instance))
  }

  def serialize(instance: AnyRef): Array[Byte] = mapper.writeValueAsBytes(instance)

  def writeRawElement(out: DataOutputStream, className: String, dataBytes: Array[Byte]): Unit = {
    val classNameBytes = className.getBytes(StandardCharsets.UTF_8)
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

  def verifyHeader(in: DataInputStream): Boolean = {
    val magic = new Array[Byte](MAGIC_NUMBER.length)
    try {
      in.readFully(magic)
      magic.sameElements(MAGIC_NUMBER)
    } catch {
      case _: java.io.EOFException => false
    }
  }
}
