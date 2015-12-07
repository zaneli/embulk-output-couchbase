package org.embulk.output.couchbase

import com.google.inject.{Binder, Module}
import org.embulk.EmbulkEmbed
import org.embulk.exec.{ExecutionResult, GuessExecutor}
import org.embulk.plugin.{InjectedPluginSource, PluginType}
import org.embulk.spi.{InputPlugin, OutputPlugin}

object EmbulkPluginTester {

  private[this] lazy val bootstrap = new EmbulkEmbed.Bootstrap()
  bootstrap.addModules(new Module() {
    override def configure(binder: Binder): Unit = {
      GuessExecutor.registerDefaultGuessPluginTo(binder, new PluginType("csv"))
      InjectedPluginSource.registerPluginTo(binder, classOf[InputPlugin], "dummy", classOf[DummyInputPlugin])
      InjectedPluginSource.registerPluginTo(binder, classOf[OutputPlugin], "couchbase", classOf[CouchbaseOutputPlugin])
    }
  })

  private[this] lazy val embulk = bootstrap.initializeCloseable()

  def run(yml: String): ExecutionResult = {
    val config = embulk.newConfigLoader().fromYamlString(yml)
    embulk.run(config)
  }
}
