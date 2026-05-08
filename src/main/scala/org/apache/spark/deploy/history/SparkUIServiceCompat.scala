package org.apache.spark.deploy.history

import org.apache.spark.status.AppStatusStore
import org.apache.spark.util.kvstore.KVStore

private[history] object SparkUIServiceCompat {

  def createAppStatusStore(store: KVStore): AppStatusStore = {
    val constructors = classOf[AppStatusStore].getConstructors

    constructors.find(_.getParameterCount == 2) match {
      case Some(ctor) =>
        ctor.newInstance(store, None).asInstanceOf[AppStatusStore]
      case None =>
        constructors.find(_.getParameterCount == 3) match {
          case Some(ctor) =>
            ctor.newInstance(store, None, None).asInstanceOf[AppStatusStore]
          case None =>
            throw new IllegalStateException(
              s"Unsupported AppStatusStore constructor set: ${constructors.mkString(", ")}")
        }
    }
  }
}
