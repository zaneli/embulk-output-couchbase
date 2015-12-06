package org.embulk.output.couchbase

import java.util.{List => JList}

import com.google.common.base.Optional
import org.embulk.config.{Config, ConfigDefault, ConfigDiff, ConfigSource, Task, TaskReport, TaskSource}
import org.embulk.spi.{Exec, OutputPlugin, Schema, TransactionalPageOutput}

class CouchbaseOutputPlugin extends OutputPlugin {

  trait PluginTask extends Task {
    // configuration option 1 (required integer)
    @Config("option1")
    def getOption1: Int

    // configuration option 2 (optional string, null is not allowed)
    @Config("option2")
    @ConfigDefault("\"myvalue\"")
    def getOption2: String

    // configuration option 3 (optional string, null is allowed)
    @Config("option3")
    @ConfigDefault("null")
    def getOption3: Optional[String]
  }

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
