package org.embulk.output.couchbase.helper

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import org.embulk.spi.{Column, PageReader}

class ColumnResolver(idColumn: String, idFormat: Option[String]) {

  def createDocument(reader: PageReader, columns: Seq[Column]): Option[JsonDocument] = {
    for {
      idColumn <- columns.find(_.getName == idColumn)
      id <- ColumnValue.resolve(reader, idColumn)
    } yield {
      val json = columns
        .filterNot(_.getName == idColumn.getName)
        .flatMap(ColumnValue.resolve(reader, _))
        .foldLeft(JsonObject.create()){ case (j, c) => j.put(c.name, c.value) }
      JsonDocument.create(toId(id), json)
    }
  }

  private[this] def toId(column: ResolvedColumn[_]): String = {
    val id = column.value.toString
    idFormat.fold(id)(_.replaceAll("""\{id\}""", id))
  }

  private[this] sealed trait ResolvedColumn[A] {
    def name: String
    def value: A
  }
  private[this] object ColumnValue {
    def resolve(reader: PageReader, column: Column): Option[ResolvedColumn[_]] = {
      if (reader.isNull(column)) {
        None
      } else {
        PartialFunction.condOpt(column.getType.getName) {
          case "string" => StringColumn(column.getName, reader.getString(column))
          case "double" => DoubleColumn(column.getName, reader.getDouble(column))
          case "long" => LongColumn(column.getName, reader.getLong(column))
          case "boolean" => BooleanColumn(column.getName, reader.getBoolean(column))
          case "timestamp" => LongColumn(column.getName, reader.getTimestamp(column).toEpochMilli) // TODO: appropriate type?
        }
      }
    }
  }
  private[this] case class StringColumn(
    override val name: String,
    override val value: String) extends ResolvedColumn[String]
  private[this] case class DoubleColumn(
    override val name: String,
    override val value: Double) extends ResolvedColumn[Double]
  private[this] case class LongColumn(
    override val name: String,
    override val value: Long) extends ResolvedColumn[Long]
  private[this] case class BooleanColumn(
    override val name: String,
    override val value: Boolean) extends ResolvedColumn[Boolean]
}
