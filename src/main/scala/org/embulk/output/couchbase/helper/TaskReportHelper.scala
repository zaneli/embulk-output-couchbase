package org.embulk.output.couchbase.helper

import org.embulk.config.TaskReport
import org.embulk.spi.Exec

object TaskReportHelper {

  def createReport(successCount: Long, failures: Map[String, Throwable]): TaskReport = {
    val report = Exec.newTaskReport()
    report.set("rans", successCount + failures.size)
    if (failures.nonEmpty) {
      report.set("failures", createFailureReports(failures))
    }
    report
  }

  private[this] def createFailureReports(failures: Map[String, Throwable]): Array[TaskReport] = {
    failures.toMap.map { case (id, t) =>
      val r = Exec.newTaskReport()
      r.set("id", id)
      r.set("cause", t.toString)
      r
    }.toArray
  }
}
