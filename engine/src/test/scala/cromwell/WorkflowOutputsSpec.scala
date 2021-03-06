package cromwell

import akka.testkit._
import cromwell.util.SampleWdl
import cromwell.CromwellTestkitSpec.AnyValueIsFine

import scala.language.postfixOps

class WorkflowOutputsSpec extends CromwellTestkitSpec {
  "Workflow outputs" should {
    "use all outputs if none are specified" in {
      runWdlAndAssertOutputs(
        sampleWdl = SampleWdl.ThreeStep,
        eventFilter = EventFilter.info(pattern = s"starting calls: three_step.cgrep, three_step.wc", occurrences = 1),
        runtime = "",
        expectedOutputs = Map(
          "three_step.ps.procs" -> AnyValueIsFine,
          "three_step.cgrep.count" -> AnyValueIsFine,
          "three_step.wc.count" -> AnyValueIsFine
        ),
        allowOtherOutputs = false
      )
    }

    "Respect the workflow output section" in {
      runWdlAndAssertOutputs(
        sampleWdl = SampleWdl.ThreeStepWithOutputsSection,
        eventFilter = EventFilter.info(pattern = s"starting calls: three_step.cgrep, three_step.wc", occurrences = 1),
        runtime = "",
        expectedOutputs = Map(
          "three_step.cgrep.count" -> AnyValueIsFine,
          "three_step.wc.count" -> AnyValueIsFine
        ),
        allowOtherOutputs = false
      )
    }

    "Not list scatter shards" in {
      runWdlAndAssertOutputs(
        sampleWdl = SampleWdl.SimpleScatterWdl,
        eventFilter = EventFilter.info(pattern = s"starting calls: scatter0.inside_scatter", occurrences = 1),
        runtime = "",
        expectedOutputs = Map(
          "scatter0.outside_scatter.out" -> AnyValueIsFine,
          "scatter0.inside_scatter.out" -> AnyValueIsFine
        ),
        allowOtherOutputs = false
      )
    }

    "Not list scatter shards, even for wildcards" in {
      runWdlAndAssertOutputs(
        sampleWdl = SampleWdl.SimpleScatterWdlWithOutputs,
        eventFilter = EventFilter.info(pattern = s"starting calls: scatter0.inside_scatter", occurrences = 1),
        runtime = "",
        expectedOutputs = Map(
          "scatter0.inside_scatter.out" -> AnyValueIsFine
        ),
        allowOtherOutputs = false
      )
    }

    "Allow explicitly named inputs in the output section" in {
      runWdlAndAssertOutputs(
        sampleWdl = SampleWdl.ThreeStepWithInputsInTheOutputsSection,
        eventFilter = EventFilter.info(pattern = s"starting calls: three_step.cgrep, three_step.wc", occurrences = 1),
        runtime = "",
        expectedOutputs = Map(
          "three_step.cgrep.pattern" -> AnyValueIsFine
        ),
        allowOtherOutputs = false
      )
    }
  }
}
