package org.embulk.output.couchbase

import com.typesafe.config.ConfigFactory

object EnvConfig {

  private[this] val config = ConfigFactory.load("env")

  val Host = getNonEmptyValue("host")
  val Bucket = config.getString("bucket")
  val Password = getNonEmptyValue("password")

  private[this] def getNonEmptyValue(path: String): Option[String] = {
    if (config.hasPath(path)) {
      Option(config.getString(path)).filter(_.nonEmpty)
    } else {
      None
    }
  }
}
