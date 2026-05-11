package org.apache.spark.deploy.history

import java.util.Collection

import scala.collection.mutable

import org.apache.spark.status.TaskDataWrapper
import org.apache.spark.util.kvstore.{KVStore, KVStoreIterator, KVStoreView}

private[history] class UIMetaShardStore(
    delegate: KVStore,
    manifest: UIMetaV2Manifest,
    reader: UIMetaV2Reader) extends KVStore {

  private val loadedStages = mutable.Set.empty[(Int, Int)]

  override def getMetadata[T](klass: Class[T]): T = delegate.getMetadata(klass)

  override def setMetadata(value: Any): Unit = delegate.setMetadata(value)

  override def read[T](klass: Class[T], naturalKey: Any): T = delegate.read(klass, naturalKey)

  override def write(value: Any): Unit = delegate.write(value)

  override def delete(klass: Class[_], naturalKey: Any): Unit = {
    delegate.delete(klass, naturalKey)
  }

  override def view[T](klass: Class[T]): KVStoreView[T] = {
    if (klass == classOf[TaskDataWrapper]) {
      new UIMetaTaskKVStoreView[T](() => delegate.view(klass), loadStageIfNeeded)
    } else {
      delegate.view(klass)
    }
  }

  override def count(klass: Class[_]): Long = delegate.count(klass)

  override def count(klass: Class[_], index: String, indexedValue: Any): Long = {
    if (klass == classOf[TaskDataWrapper]) {
      loadStageIfNeeded(index, indexedValue)
      if (index == "stage" && stageAttemptFromIndex(indexedValue).isDefined &&
          delegate.count(klass) == 0L) {
        return 0L
      }
    }
    delegate.count(klass, index, indexedValue)
  }

  override def removeAllByIndexValues[T](
      klass: Class[T],
      index: String,
      indexValues: Collection[_]): Boolean = {
    delegate.removeAllByIndexValues(klass, index, indexValues)
  }

  override def close(): Unit = delegate.close()

  private def loadStageIfNeeded(index: String, value: Any): Boolean = synchronized {
    if (index != "stage") {
      return false
    }

    stageAttemptFromIndex(value).exists { case (stageId, attemptId) =>
      val key = stageId -> attemptId
      if (loadedStages.contains(key)) {
        true
      } else {
        val shards = manifest.taskShardsFor(stageId, attemptId)
        if (shards.isEmpty) {
          loadedStages += key
          false
        } else {
          reader.loadShards(manifest, shards, delegate)
          loadedStages += key
          true
        }
      }
    }
  }

  private def stageAttemptFromIndex(value: Any): Option[(Int, Int)] = value match {
    case values: Array[_] if values.length == 2 =>
      for {
        stageId <- toInt(values(0))
        attemptId <- toInt(values(1))
      } yield (stageId, attemptId)
    case values: Seq[_] if values.length == 2 =>
      for {
        stageId <- toInt(values.head)
        attemptId <- toInt(values(1))
      } yield (stageId, attemptId)
    case _ =>
      None
  }

  private def toInt(value: Any): Option[Int] = value match {
    case n: Number if n.longValue() >= Int.MinValue && n.longValue() <= Int.MaxValue =>
      Some(n.intValue())
    case _ => None
  }
}

private[history] class UIMetaTaskKVStoreView[T](
    newDelegate: () => KVStoreView[T],
    loadStageIfNeeded: (String, Any) => Boolean) extends KVStoreView[T] {

  private val operations = mutable.ArrayBuffer.empty[KVStoreView[T] => Unit]
  private var selectedIndex: Option[String] = None

  override def index(name: String): KVStoreView[T] = {
    selectedIndex = Some(name)
    operations += { view => view.index(name) }
    this
  }

  override def parent(value: Any): KVStoreView[T] = {
    loadStageIfNeeded("stage", value)
    operations += { view => view.parent(value) }
    this
  }

  override def first(value: Any): KVStoreView[T] = {
    selectedIndex.foreach { index => loadStageIfNeeded(index, value) }
    operations += { view => view.first(value) }
    this
  }

  override def last(value: Any): KVStoreView[T] = {
    selectedIndex.foreach { index => loadStageIfNeeded(index, value) }
    operations += { view => view.last(value) }
    this
  }

  override def reverse(): KVStoreView[T] = {
    operations += { view => view.reverse() }
    this
  }

  override def max(max: Long): KVStoreView[T] = {
    operations += { view => view.max(max) }
    this
  }

  override def skip(n: Long): KVStoreView[T] = {
    operations += { view => view.skip(n) }
    this
  }

  override def iterator(): java.util.Iterator[T] = currentView.iterator()

  override def closeableIterator(): KVStoreIterator[T] = currentView.closeableIterator()

  private def currentView: KVStoreView[T] = {
    val view = newDelegate()
    operations.foreach(_(view))
    view
  }
}
