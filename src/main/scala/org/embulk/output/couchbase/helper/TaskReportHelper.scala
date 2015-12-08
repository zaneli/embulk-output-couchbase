package org.embulk.output.couchbase.helper

import org.embulk.config.TaskReport
import org.embulk.spi.Exec

object TaskReportHelper {

  def createReport(successCount: Long, failures: Map[String, Throwable]): TaskReport = {
    val r = Exec.newTaskReport.set("rans", successCount + failures.size)
    if (failures.nonEmpty) {
      r.set("failures", createFailureReports(failures))
    } else {
      r
    }
  }

  private[this] def createFailureReports(failures: Map[String, Throwable]): Array[TaskReport] = {
    failures.toMap.map { case (id, t) =>
      Exec.newTaskReport.set("id", id).set("cause", t.toString)
    }.toArray
  }
}
