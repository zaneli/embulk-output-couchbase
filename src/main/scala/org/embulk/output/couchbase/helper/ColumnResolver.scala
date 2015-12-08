package org.embulk.output.couchbase.helper

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import org.embulk.spi.{Column, PageReader}
import org.embulk.spi.`type`.Types.{BOOLEAN, DOUBLE, LONG, STRING, TIMESTAMP}

class ColumnResolver(idColumn: String, idFormat: Option[String]) {

  def createDocument(reader: PageReader, columns: Seq[Column]): Option[JsonDocument] = {
    for {
      idColumn <- columns.find(_.getName == idColumn)
      id <- ColumnValue.resolve(reader, idColumn)
    } yield {
      val content = columns
        .filterNot(_ == idColumn)
        .flatMap(ColumnValue.resolve(reader, _))
        .foldLeft(JsonObject.create){ case (j, c) => j.put(c.name, c.value) }
      JsonDocument.create(toId(id), content)
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
        PartialFunction.condOpt(column.getType) {
          case STRING => StringColumn(column.getName, reader.getString(column))
          case DOUBLE => DoubleColumn(column.getName, reader.getDouble(column))
          case LONG => LongColumn(column.getName, reader.getLong(column))
          case BOOLEAN => BooleanColumn(column.getName, reader.getBoolean(column))
          case TIMESTAMP => LongColumn(column.getName, reader.getTimestamp(column).toEpochMilli) // TODO: appropriate type?
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
