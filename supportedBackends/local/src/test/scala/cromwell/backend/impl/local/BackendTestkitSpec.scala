package cromwell.backend.impl.local

import java.nio.file.FileSystems

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import com.typesafe.config.ConfigFactory
import cromwell.backend.BackendJobExecutionActor.{BackendJobExecutionFailedResponse, BackendJobExecutionFailedRetryableResponse, BackendJobExecutionResponse, BackendJobExecutionSucceededResponse}
import cromwell.backend._
import cromwell.backend.impl.local.TestWorkflows.TestWorkflow
import cromwell.core.{WorkflowId, WorkflowOptions}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, Tag}
import spray.json.{JsObject, JsValue}
import wdl4s._
import wdl4s.expression.WdlFunctions
import wdl4s.values.WdlValue

import scala.util.Try

object BackendTestkitSpec {
  implicit val testActorSystem = ActorSystem("LocalBackendSystem")
  object DockerTest extends Tag("DockerTest")

}

trait BackendTestkitSpec extends ScalaFutures with Matchers {
  import BackendTestkitSpec._

  val localFileSystem = List(FileSystems.getDefault)
  val defaultBackendConfig = new BackendConfigurationDescriptor("config", ConfigFactory.load())
  val defaultConfig = defaultBackendConfig.config.getConfig(defaultBackendConfig.configPath)

  implicit val defaultPatience = PatienceConfig(timeout = Span(5, Seconds), interval = Span(500, Millis))

  private def splitFqn(fullyQualifiedName: FullyQualifiedName): (String, String) = {
    val lastIndex = fullyQualifiedName.lastIndexOf(".")
    (fullyQualifiedName.substring(0, lastIndex), fullyQualifiedName.substring(lastIndex + 1))
  }

  def testWorkflow(workflow: TestWorkflow, symbols: Map[LocallyQualifiedName, WdlValue] = Map.empty) = {
    val backend = localBackend(jobDescriptorFromSingleCallWorkflow(workflow.workflowDescriptor, symbols), workflow.config)
    executeJobAndAssertOutputs(backend, workflow.expectedResponse)
  }

  def buildWorkflowDescriptor(wdl: WdlSource,
                              inputs: Map[String, WdlValue] = Map.empty,
                              options: WorkflowOptions = WorkflowOptions(JsObject(Map.empty[String, JsValue])),
                              runtime: String = "") = {
    new BackendWorkflowDescriptor(
      WorkflowId.randomId(),
      NamespaceWithWorkflow.load(wdl.replaceAll("RUNTIME", runtime)),
      inputs,
      options
    )
  }

  def localBackend(jobDescriptor: BackendJobDescriptor, conf: BackendConfigurationDescriptor) = {
    TestActorRef(new LocalJobExecutionActor(jobDescriptor, conf)).underlyingActor
  }

  /**
    * Gather all useful (and only those) inputs for this call from the JSON mappings.
    */
  def unqualifyInputs(fqInputs: Map[LocallyQualifiedName, WdlValue]): Map[LocallyQualifiedName, WdlValue] = {
    // inputs contains evaluated workflow level declarations and coerced json inputs.
    // This is done during Materialization of WorkflowDescriptor
    val splitFqns = fqInputs map {
      case (fqn, v) => splitFqn(fqn) -> v
    }
    splitFqns collect {
      case((root, inputName), v) => inputName -> v // Variables are looked up with LQNs, not FQNs
    }
  }

  def jobDescriptorFromSingleCallWorkflow(workflowDescriptor: BackendWorkflowDescriptor, symbols: Map[LocallyQualifiedName, WdlValue] = Map.empty) = {
    val symbolsMap = symbols ++ unqualifyInputs(workflowDescriptor.inputs)
    val call = workflowDescriptor.workflowNamespace.workflow.calls.head
    val jobKey = new BackendJobDescriptorKey(call, None, 1)
    new BackendJobDescriptor(workflowDescriptor, jobKey, symbolsMap)
  }

  def assertResponse(executionResponse: BackendJobExecutionResponse, expectedResponse: BackendJobExecutionResponse) = {
    (executionResponse, expectedResponse) match {
      case (BackendJobExecutionSucceededResponse(_, responseOutputs), BackendJobExecutionSucceededResponse(_, expectedOutputs)) =>
        responseOutputs.size shouldBe expectedOutputs.size
        responseOutputs foreach {
          case (fqn, out) =>
            val expectedOut = expectedOutputs.get(fqn)
            expectedOut.isDefined shouldBe true
            expectedOut.get.wdlValue.valueString shouldBe out.wdlValue.valueString
        }
      case (BackendJobExecutionFailedResponse(_, failure), BackendJobExecutionFailedResponse(_, expectedFailure)) =>
        // TODO improve this
        failure.getClass shouldBe expectedFailure.getClass
      case (BackendJobExecutionFailedRetryableResponse(_, failure), BackendJobExecutionFailedRetryableResponse(_, expectedFailure)) =>
        failure.getClass shouldBe expectedFailure.getClass
      case (response, expectation) =>
        fail(s"Execution response $response wasn't conform to expectation $expectation")
    }
  }

  def executeJobAndAssertOutputs(backend: BackendJobExecutionActor, expectedResponse: BackendJobExecutionResponse) = {
    whenReady(backend.execute) { executionResponse =>
      assertResponse(executionResponse, expectedResponse)
    }
  }

}
