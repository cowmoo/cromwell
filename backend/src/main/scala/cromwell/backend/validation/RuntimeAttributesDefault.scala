package cromwell.backend.validation

import cromwell.core.{EvaluatedRuntimeAttributes, WorkflowOptions}
import wdl4s.types.WdlType
import wdl4s.util.TryUtil
import wdl4s.values.WdlValue

import scala.util.Try

object RuntimeAttributesDefault {

  def workflowOptionsDefault(options: WorkflowOptions, mapping: Map[String, WdlType]): Try[Map[String, WdlValue]] = {
    options.defaultRuntimeOptions flatMap { attrs =>
      TryUtil.sequenceMap(attrs collect {
        case (k, v) if mapping.contains(k) => k -> mapping(k).coerceRawValue(v)
      })
    }
  }

  /**
    * Traverse defaultsList in order, and for each of them add the missing (and only missing) runtime attributes.
   */
  def foldDefaults(attributes: EvaluatedRuntimeAttributes, defaultsList: List[EvaluatedRuntimeAttributes]): EvaluatedRuntimeAttributes = {
    defaultsList.foldLeft(attributes)((acc, default) => {
      acc ++ default.filterKeys(!acc.keySet.contains(_))
    })
  }

}
