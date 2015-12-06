package org.embulk.output.couchbase

import java.util.concurrent.atomic.AtomicLong

import com.couchbase.client.java.CouchbaseCluster
import org.embulk.config.TaskReport
import org.embulk.output.couchbase.helper.ColumnResolver
import org.embulk.output.couchbase.helper.TaskReportHelper._
import org.embulk.spi.{Exec, Page, PageReader, Schema, TransactionalPageOutput}

import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.util.control.NonFatal

class CouchbasePageOutput(task: CouchbaseTask, schema: Schema) extends TransactionalPageOutput {

  private[this] val logger = Exec.getLogger(this.getClass)
  private[this] val successCount = new AtomicLong
  private[this] val failures = TrieMap.empty[String, Throwable]

  private[this] lazy val cluster = {
    task.host.fold(CouchbaseCluster.create())(CouchbaseCluster.create(_))
  }

  private[this] lazy val bucket = {
    val name = task.bucket
    task.password.fold(cluster.openBucket(name))(cluster.openBucket(name, _))
  }

  override def add(page: Page): Unit = {
    val columns = schema.getColumns.asScala
    val reader = new PageReader(schema)
    reader.setPage(page)
    val resolver = new ColumnResolver(task.idColumn, task.idFormat)
    Iterator.continually(reader.nextRecord).takeWhile(_ == true).flatMap { _ =>
      resolver.createDocument(reader, columns)
    }.toList.foreach { document =>
      try {
        task.writeMode.write(bucket, document)
        successCount.addAndGet(1L)
      } catch {
        case NonFatal(e) =>
          val id = document.id()
          logger.error(s"$id failed", e)
          failures += id -> e
      }
    }
  }

  override def finish(): Unit = {}

  override def close(): Unit = {
    bucket.close()
    cluster.disconnect()
  }

  override def abort(): Unit = {}

  override def commit(): TaskReport = {
    createReport(successCount.longValue(), failures.toMap)
  }
}
