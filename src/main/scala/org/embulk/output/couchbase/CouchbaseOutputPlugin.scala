package org.embulk.output.couchbase

import java.util.{List => JList}

import org.embulk.config.{ConfigDiff, ConfigSource, TaskReport, TaskSource}
import org.embulk.spi.{Exec, OutputPlugin, Schema, TransactionalPageOutput}

import scala.collection.JavaConversions._

class CouchbaseOutputPlugin extends OutputPlugin {

  override def transaction(
    config: ConfigSource, schema: Schema, taskCount: Int, control: OutputPlugin.Control): ConfigDiff = {
    val task = config.loadConfig(classOf[PluginTask])
    CouchbaseTask.checkConfig(task, schema)
    control.run(task.dump).foldLeft(Exec.newConfigDiff)(_.merge(_))
  }

  override def resume(
    taskSource: TaskSource, schema: Schema, taskCount: Int, control: OutputPlugin.Control): ConfigDiff = {
    throw new UnsupportedOperationException("couchbase output plugin does not support resuming")
  }

  override def cleanup(
    taskSource: TaskSource,
    schema: Schema,
    taskCount: Int,
    successTaskReports: JList[TaskReport]): Unit = {}

  override def open(taskSource: TaskSource, schema: Schema, taskIndex: Int): TransactionalPageOutput = {
    val task = CouchbaseTask(taskSource.loadTask(classOf[PluginTask]))
    new CouchbasePageOutput(task, schema)
  }
}
