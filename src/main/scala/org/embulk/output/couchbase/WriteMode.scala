package org.embulk.output.couchbase

import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.Document

sealed abstract class WriteMode(val value: String) {
  def write[A <: Document[_]](bucket: Bucket, document: A): A
}

object WriteMode {

  def of(value: String): Option[WriteMode] = PartialFunction.condOpt(value) {
    case Insert.value => Insert
    case Upsert.value => Upsert
  }

  case object Insert extends WriteMode("insert") {
    override def write[A <: Document[_]](bucket: Bucket, document: A): A = {
      bucket.insert(document)
    }
  }

  case object Upsert extends WriteMode("upsert") {
    override def write[A <: Document[_]](bucket: Bucket, document: A): A = {
      bucket.upsert(document)
    }
  }
}
