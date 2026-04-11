package org.apache.spark.deploy.history

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.scalatest.funsuite.AnyFunSuite

class DummyWrapper(val id: Int, val name: String) extends Serializable {
  override def equals(obj: Any): Boolean = obj match {
    case d: DummyWrapper => d.id == id && d.name == name
    case _ => false
  }
}

class UIMetaFileSuite extends AnyFunSuite {

  test("write and read elements round-trip") {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)

    val obj1 = new DummyWrapper(1, "alpha")
    val obj2 = new DummyWrapper(2, "beta")

    UIMetaFile.writeHeader(dos)
    UIMetaFile.writeElement(dos, classOf[DummyWrapper].getName, obj1)
    UIMetaFile.writeElement(dos, classOf[DummyWrapper].getName, obj2)
    dos.flush()

    val dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray))

    assert(UIMetaFile.verifyHeader(dis))

    val e1 = UIMetaFile.readElementBytes(dis)
    assert(e1.isDefined)
    assert(UIMetaFile.deserialize(e1.get._2, classOf[DummyWrapper]) == obj1)

    val e2 = UIMetaFile.readElementBytes(dis)
    assert(e2.isDefined)
    assert(UIMetaFile.deserialize(e2.get._2, classOf[DummyWrapper]) == obj2)

    assert(UIMetaFile.readElementBytes(dis).isEmpty) // EOF
  }
}
