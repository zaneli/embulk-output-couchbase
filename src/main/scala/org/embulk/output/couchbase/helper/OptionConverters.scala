package org.embulk.output.couchbase.helper

import com.google.common.base.Optional

object OptionConverters {
  import scala.language.implicitConversions

  implicit class Optional2Option[A](val underlying: Optional[A]) extends AnyVal {
    def asScala: Option[A] = PartialFunction.condOpt(underlying) {
      case o if o.isPresent => o.get
    }
  }
}
