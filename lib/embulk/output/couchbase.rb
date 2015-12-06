Embulk::JavaPlugin.register_output(
  "couchbase", "org.embulk.output.couchbase.CouchbaseOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
