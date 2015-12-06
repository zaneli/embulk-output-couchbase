package org.embulk.output.couchbase

import com.google.common.base.Optional
import org.embulk.config.{Config, ConfigDefault, ConfigException, Task}

trait PluginTask extends Task {
  @Config("host")
  @ConfigDefault("null")
  def getHost: Optional[String]

  @Config("bucket")
  def getBucket: String

  @Config("password")
  @ConfigDefault("null")
  def getPassword: Optional[String]

  @Config("id_column")
  def getIdColumn: String
  @Config("id_format")
  @ConfigDefault("null")
  def getIdFormat: Optional[String]

  @Config("write_mode")
  @ConfigDefault("null")
  def getWriteMode: Optional[String]
}

case class CouchbaseTask(
  host: Option[String],
  password: Option[String],
  bucket: String,
  idColumn: String,
  idFormat: Option[String],
  writeMode: WriteMode)

object CouchbaseTask {
  import org.embulk.output.couchbase.helper.OptionConverters._

  def apply(task: PluginTask): CouchbaseTask = {
    CouchbaseTask(
      task.getHost.asScala,
      task.getPassword.asScala,
      task.getBucket,
      task.getIdColumn,
      task.getIdFormat.asScala,
      task.getWriteMode.asScala.map(WriteMode.of).getOrElse(WriteMode.Insert)
    )
  }

  def checkConfig(task: PluginTask): Unit = {
    task.getIdFormat.asScala.foreach { format =>
      if (!format.contains("{id}")) {
        throw new ConfigException(s"Invalid id_format '$format'. must contains '{id}'")
      }
    }
    task.getWriteMode.asScala.foreach(WriteMode.of)
  }
}
