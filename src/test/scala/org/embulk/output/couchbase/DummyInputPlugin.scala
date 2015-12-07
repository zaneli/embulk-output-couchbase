package org.embulk.output.couchbase

import java.util.{List => JList}

import org.embulk.config.{Config, ConfigDiff, ConfigInject, ConfigSource, Task, TaskReport, TaskSource}
import org.embulk.spi.{BufferAllocator, Exec, InputPlugin, PageBuilder, PageOutput, Schema, SchemaConfig}
import org.embulk.spi.InputPlugin.Control
import org.embulk.spi.time.Timestamp
import org.embulk.spi.`type`.Types.{BOOLEAN, DOUBLE, LONG, STRING, TIMESTAMP}

import scala.collection.JavaConversions._

class DummyInputPlugin extends InputPlugin {

  trait PluginTask extends Task {
    @Config("data")
    def getData: String
    @Config("columns")
    def getColumns: SchemaConfig
    @ConfigInject
    def getBufferAllocator: BufferAllocator
  }

  override def transaction(config: ConfigSource, control: Control): ConfigDiff = {
    val task = config.loadConfig(classOf[PluginTask])
    val schema = task.getColumns.toSchema
    val taskCount = 1
    resume(task.dump(), schema, taskCount, control)
  }

  override def resume(taskSource: TaskSource, schema: Schema, taskCount: Int, control: Control): ConfigDiff = {
    control.run(taskSource, schema, taskCount)
    Exec.newConfigDiff()
  }

  override def cleanup(
    taskSource: TaskSource, schema: Schema, taskCount: Int, successTaskReports: JList[TaskReport]): Unit = {}

  override def run(taskSource: TaskSource, schema: Schema, taskIndex: Int, output: PageOutput): TaskReport = {
    val task = taskSource.loadTask(classOf[PluginTask])
    buildPage(task, schema, output)
    Exec.newTaskReport()
  }

  override def guess(config: ConfigSource): ConfigDiff = {
    Exec.newConfigDiff()
  }

  private[this] def buildPage(task: PluginTask, schema: Schema, output: PageOutput): Unit = {
    val dummyData = task.getData.split('|').map(_.split(','))
    val allocator = task.getBufferAllocator
    val builder = new PageBuilder(allocator, schema, output)
    dummyData.foreach { row =>
      row.zip(builder.getSchema.getColumns).foreach { case (cell, column) =>
        column.getType match {
          case BOOLEAN => builder.setBoolean(column, cell.toBoolean)
          case DOUBLE => builder.setDouble(column, cell.toDouble)
          case LONG => builder.setLong(column, cell.toLong)
          case STRING => builder.setString(column, cell)
          case TIMESTAMP =>  builder.setTimestamp(column, Timestamp.ofEpochMilli(cell.toLong))
        }
      }
      builder.addRecord()
    }
    builder.finish()
  }
}
