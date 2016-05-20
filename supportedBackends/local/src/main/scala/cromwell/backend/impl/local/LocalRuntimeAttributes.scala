package cromwell.backend.impl.local

import cromwell.backend.validation.RuntimeAttributesDefault._
import cromwell.backend.validation.RuntimeAttributesKeys._
import cromwell.backend.validation.RuntimeAttributesValidation._
import cromwell.backend.validation.{ContinueOnReturnCode, ContinueOnReturnCodeSet}
import cromwell.core.WorkflowOptions
import lenthall.exception.MessageAggregation
import wdl4s.values.{WdlBoolean, WdlInteger, WdlValue}

import scalaz.Scalaz._
import scalaz._

object LocalRuntimeAttributes {
  val FailOnStderrDefaultValue = false
  val ContinueOnRcDefaultValue = 0

  val staticDefaults = Map(
    FailOnStderr -> WdlBoolean(false),
    ContinueOnReturnCode -> WdlInteger(0)
  )

  def apply(attrs: Map[String, WdlValue], options: WorkflowOptions): LocalRuntimeAttributes = {
    // Fail now if some workflow options are specified but can't be parsed correctly
    val defaultFromOptions = workflowOptionsDefault(options, staticDefaults.mapValues(_.wdlType)).get
    val attrsWithDefaults = foldDefaults(attrs, List(defaultFromOptions, staticDefaults))

    val docker = validateDocker(attrsWithDefaults.get(Docker), None.successNel)
    val failOnStderr = validateFailOnStderr(attrsWithDefaults.get(FailOnStderr), FailOnStderrDefaultValue.successNel)
    val continueOnReturnCode = validateContinueOnReturnCode(attrsWithDefaults.get(ContinueOnReturnCode),
      ContinueOnReturnCodeSet(Set(ContinueOnRcDefaultValue)).successNel)
    (continueOnReturnCode |@| docker |@| failOnStderr) {
      new LocalRuntimeAttributes(_, _, _)
    } match {
      case Success(x) => x
      case Failure(nel) => throw new RuntimeException with MessageAggregation {
        override def exceptionContext: String = "Runtime attribute validation failed"
        override def errorMessages: Traversable[String] = nel.list
      }
    }
  }
}

case class LocalRuntimeAttributes(continueOnReturnCode: ContinueOnReturnCode, dockerImage: Option[String], failOnStderr: Boolean)
