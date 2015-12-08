package org.embulk.output.couchbase

import com.typesafe.config.{Config, ConfigFactory}

object EnvConfig {
  private[this] val config = ConfigFactory.load("env")

  val Host = config.getNonEmptyStringOpt("host")
  val Bucket = config.getString("bucket")
  val Password = config.getNonEmptyStringOpt("password")

  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getNonEmptyStringOpt(path: String): Option[String] = {
      if (underlying.hasPath(path)) {
        Option(underlying.getString(path)).filter(_.nonEmpty)
      } else {
        None
      }
    }
  }
}
