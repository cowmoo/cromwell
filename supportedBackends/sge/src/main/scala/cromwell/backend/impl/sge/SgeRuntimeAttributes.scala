package cromwell.backend.impl.sge

import cromwell.backend.validation.{ContinueOnReturnCodeSet, ContinueOnReturnCode}
import cromwell.backend.validation.RuntimeAttributesKeys._
import cromwell.backend.validation.RuntimeAttributesValidation._
import lenthall.exception.MessageAggregation
import wdl4s.values.WdlValue

import scalaz._
import Scalaz._

object SgeRuntimeAttributes {
  val FailOnStderrDefaultValue = false
  val ContinueOnRcDefaultValue = 0

  def apply(attrs: Map[String, WdlValue]): SgeRuntimeAttributes = {
    val docker = validateDocker(attrs.get(DockerKey), None.successNel)
    val failOnStderr = validateFailOnStderr(attrs.get(FailOnStderrKey), FailOnStderrDefaultValue.successNel)
    val continueOnReturnCode = validateContinueOnReturnCode(attrs.get(ContinueOnReturnCodeKey),
      ContinueOnReturnCodeSet(Set(ContinueOnRcDefaultValue)).successNel)
    (continueOnReturnCode |@| docker |@| failOnStderr) {
      new SgeRuntimeAttributes(_, _, _)
    } match {
      case Success(x) => x
      case Failure(nel) => throw new RuntimeException with MessageAggregation {
        override def exceptionContext: String = "Runtime attribute validation failed"
        override def errorMessages: Traversable[String] = nel.list
      }
    }
  }
}

case class SgeRuntimeAttributes(continueOnReturnCode: ContinueOnReturnCode, dockerImage: Option[String], failOnStderr: Boolean)
