package org.embulk.output.couchbase

import com.couchbase.client.java.CouchbaseCluster
import org.embulk.exec.PartialExecutionException
import org.embulk.output.couchbase.EnvConfig.{Bucket => bucketName}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSpec}

import scala.collection.mutable.ListBuffer
import scala.util.control.Exception.ignoring

class CouchbaseOutputPluginSpec extends FunSpec with BeforeAndAfter with BeforeAndAfterAll {

  private[this] lazy val cluster = {
    EnvConfig.Host.fold(CouchbaseCluster.create())(CouchbaseCluster.create(_))
  }
  private[this] lazy val bucket = {
    EnvConfig.Password.fold(cluster.openBucket(bucketName))(cluster.openBucket(bucketName, _))
  }

  private[this] val ids = ListBuffer.empty[String]

  after {
    ids.foreach { id =>
      ignoring(classOf[Exception]) {
        bucket.remove(id)
      }
    }
    ids.clear()
  }

  override protected[this] def afterAll(): Unit = {
    ignoring(classOf[Exception]) {
      bucket.close()
    }
    ignoring(classOf[Exception]) {
      cluster.disconnect()
    }
  }

  describe("insert") {
    val writeMode = WriteMode.Insert.value

    it("output data") {
      val yml =
        s"""
          |in:
          |  type: dummy
          |  data: "1,test1,170.1|2,test2,170.2"
          |  columns:
          |  - {name: id, type: long}
          |  - {name: name, type: string}
          |  - {name: height, type: double}
          |out:
          |  type: couchbase
          |  bucket: $bucketName
          |  id_column: id
          |  id_format: embulk_${writeMode}_test1_{id}
        """.stripMargin

      ids ++= Seq(s"embulk_${writeMode}_test1_1", s"embulk_${writeMode}_test1_2")

      EmbulkPluginTester.run(yml)
      assertDocument(s"embulk_${writeMode}_test1_1", Map("name" -> "test1", "height" -> 170.1))
      assertDocument(s"embulk_${writeMode}_test1_2", Map("name" -> "test2", "height" -> 170.2))
    }

    it("output duplicated id's data") {
      val yml =
        s"""
           |in:
           |  type: dummy
           |  data: "1,test1,170.1|2,test2,170.2|2,duplicated2,180.2|3,test3,170.3"
           |  columns:
           |  - {name: id, type: long}
           |  - {name: name, type: string}
           |  - {name: height, type: double}
           |out:
           |  type: couchbase
           |  bucket: $bucketName
           |  id_column: id
           |  id_format: embulk_${writeMode}_test2_{id}
        """.stripMargin

      ids ++= Seq(s"embulk_${writeMode}_test2_1", s"embulk_${writeMode}_test2_2", s"embulk_${writeMode}_test2_3")

      EmbulkPluginTester.run(yml)
      assertDocument(s"embulk_${writeMode}_test2_1", Map("name" -> "test1", "height" -> 170.1))
      assertDocument(s"embulk_${writeMode}_test2_2", Map("name" -> "test2", "height" -> 170.2))
      assertDocument(s"embulk_${writeMode}_test2_3", Map("name" -> "test3", "height" -> 170.3))
    }
  }

  describe("upsert") {
    val writeMode = WriteMode.Upsert.value

    it("output data") {
      val yml =
        s"""
           |in:
           |  type: dummy
           |  data: "1,test1,170.1|2,test2,170.2"
           |  columns:
           |  - {name: id, type: long}
           |  - {name: name, type: string}
           |  - {name: height, type: double}
           |out:
           |  type: couchbase
           |  bucket: $bucketName
           |  id_column: id
           |  id_format: embulk_${writeMode}_test1_{id}
           |  write_mode: $writeMode
        """.stripMargin

      ids ++= Seq(s"embulk_${writeMode}_test1_1", s"embulk_${writeMode}_test1_2")

      EmbulkPluginTester.run(yml)
      assertDocument(s"embulk_${writeMode}_test1_1", Map("name" -> "test1", "height" -> 170.1))
      assertDocument(s"embulk_${writeMode}_test1_2", Map("name" -> "test2", "height" -> 170.2))
    }

    it("output duplicated id's data") {
      val yml =
        s"""
           |in:
           |  type: dummy
           |  data: "1,test1,170.1|2,test2,170.2|2,duplicated2,180.2|3,test3,170.3"
           |  columns:
           |  - {name: id, type: long}
           |  - {name: name, type: string}
           |  - {name: height, type: double}
           |out:
           |  type: couchbase
           |  bucket: $bucketName
           |  id_column: id
           |  id_format: embulk_${writeMode}_test2_{id}
           |  write_mode: $writeMode
        """.stripMargin

      ids ++= Seq(s"embulk_${writeMode}_test2_1", s"embulk_${writeMode}_test2_2", s"embulk_${writeMode}_test2_3")

      EmbulkPluginTester.run(yml)
      assertDocument(s"embulk_${writeMode}_test2_1", Map("name" -> "test1", "height" -> 170.1))
      assertDocument(s"embulk_${writeMode}_test2_2", Map("name" -> "duplicated2", "height" -> 180.2))
      assertDocument(s"embulk_${writeMode}_test2_3", Map("name" -> "test3", "height" -> 170.3))
    }
  }

  describe("invalid config") {
    it("invalid id_column") {
      val yml =
        s"""
           |in:
           |  type: dummy
           |  data: "1,test1,170.1|2,test2,170.2"
           |  columns:
           |  - {name: id, type: long}
           |  - {name: name, type: string}
           |  - {name: height, type: double}
           |out:
           |  type: couchbase
           |  bucket: $bucketName
           |  id_column: invalid
           |  id_format: embulk_invalid_{id}
        """.stripMargin

      val thrown = intercept[PartialExecutionException] {
        EmbulkPluginTester.run(yml)
      }
      assert(thrown.getMessage === """org.embulk.config.ConfigException: Invalid id_column 'invalid'. not found.""")
    }

    it("invalid id_format") {
      val yml =
        s"""
           |in:
           |  type: dummy
           |  data: "1,test1,170.1|2,test2,170.2"
           |  columns:
           |  - {name: id, type: long}
           |  - {name: name, type: string}
           |  - {name: height, type: double}
           |out:
           |  type: couchbase
           |  bucket: $bucketName
           |  id_column: id
           |  id_format: embulk_invalid
        """.stripMargin

      val thrown = intercept[PartialExecutionException] {
        EmbulkPluginTester.run(yml)
      }
      assert(thrown.getMessage === """org.embulk.config.ConfigException: Invalid id_format 'embulk_invalid'. must contains '{id}'""")
    }

    it("invalid write_mode") {
      val yml =
        s"""
           |in:
           |  type: dummy
           |  data: "1,test1,170.1|2,test2,170.2"
           |  columns:
           |  - {name: id, type: long}
           |  - {name: name, type: string}
           |  - {name: height, type: double}
           |out:
           |  type: couchbase
           |  bucket: $bucketName
           |  id_column: id
           |  id_format: embulk_invalid_{id}
           |  write_mode: invalid
        """.stripMargin

      val thrown = intercept[PartialExecutionException] {
        EmbulkPluginTester.run(yml)
      }
      assert(thrown.getMessage === """org.embulk.config.ConfigException: Unknown write_mode 'invalid'. Supported write_mode are insert, upsert""")
    }
  }

  private[this] def assertDocument(id: String, expected: Map[String, Any]): Unit = {
    val document = bucket.get(id)
    expected.foreach { case (k, v) =>
      assert(document.content().get(k) === v, s"id=$id, name=$k, value=$v")
    }
  }
}
