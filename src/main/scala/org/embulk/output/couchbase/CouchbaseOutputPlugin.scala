package org.embulk.output.couchbase

import java.util.{List => JList}

import org.embulk.config.{ConfigDiff, ConfigSource, TaskReport, TaskSource}
import org.embulk.spi.{Exec, OutputPlugin, Schema, TransactionalPageOutput}

class CouchbaseOutputPlugin extends OutputPlugin {

  override def transaction(
    config: ConfigSource, schema: Schema, taskCount: Int, control: OutputPlugin.Control): ConfigDiff = {
    val task = config.loadConfig(classOf[PluginTask])

    // retryable (idempotent) output:
    // return resume(task.dump(), schema, taskCount, control);

    // non-retryable (non-idempotent) output:
    control.run(task.dump())
    Exec.newConfigDiff()
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
    val task = taskSource.loadTask(classOf[PluginTask])

    // Write your code here :)
    throw new UnsupportedOperationException("CouchbaseOutputPlugin.run method is not implemented yet")
  }
}
