package cromwell.backend.impl.local

import java.nio.file.FileSystems

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import com.typesafe.config.ConfigFactory
import cromwell.backend.BackendJobExecutionActor.{BackendJobExecutionFailedResponse, BackendJobExecutionFailedRetryableResponse, BackendJobExecutionResponse, BackendJobExecutionSucceededResponse}
import cromwell.backend._
import cromwell.backend.impl.local.TestWorkflows.TestWorkflow
import cromwell.core.{WorkflowId, WorkflowOptions, _}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{Matchers, Tag}
import spray.json.{JsObject, JsValue}
import wdl4s._
import wdl4s.expression.WdlEvaluator.{StringMapper, WdlValueMapper}
import wdl4s.expression.{WdlEvaluator, WdlEvaluatorBuilder, WdlFunctions}
import wdl4s.values.WdlValue

import scala.language.postfixOps

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

  def testWorkflow(workflow: TestWorkflow) = {
    val backend = localBackend(jobDescriptorFromSingleCallWorkflow(workflow.workflowDescriptor), workflow.config)
    executeJobAndAssertOutputs(backend, workflow.expectedResponse)
  }


  // FIXME this is copy/paste from the engine...
  def workflowInputsFor(workflowDescriptor: BackendWorkflowDescriptor, call: Call): Map[LocallyQualifiedName, WdlValue] = {
    // Useful inputs are workflow level inputs and inputs for this specific call
    def isUsefulInput(fqn: String) = fqn == call.fullyQualifiedName || fqn == workflowDescriptor.workflowNamespace.workflow.unqualifiedName

    // inputs contains evaluated workflow level declarations and coerced json inputs.
    // This evaluation work is done during the Materialization of WorkflowDescriptor
    val splitFqns = workflowDescriptor.inputs map {
      case (fqn, v) => fqn.splitFqn -> v
    }
    splitFqns collect {
      case((root, inputName), v) if isUsefulInput(root) => inputName -> v // Variables are looked up with LQNs, not FQNs
    }
  }

  def inputsFor(descriptor: BackendWorkflowDescriptor, call: Call): Map[LocallyQualifiedName, WdlValue] = {
    // Task declarations that have a static value assigned
    val staticDeclarations = call.task.declarations collect {
      case declaration if declaration.expression.isDefined => declaration.name -> declaration.expression.get
    } toMap

    staticDeclarations ++ workflowInputsFor(descriptor, call) ++ call.inputMappings
  }

  def buildWorkflowDescriptor(wdl: WdlSource,
                              workflowDeclarations: Map[String, WdlValue] = Map.empty,
                              inputs: Map[String, WdlValue] = Map.empty,
                              options: WorkflowOptions = WorkflowOptions(JsObject(Map.empty[String, JsValue])),
                              runtime: String = "") = {
    // Workflow declarations are evaluated in the engine. We have to pass them as an argument for now
    // When https://github.com/broadinstitute/wdl4s/pull/25 is merged we could use staticWorkflowDeclarationsRecursive with stub engine functions to evaluate them

    new BackendWorkflowDescriptor(
      WorkflowId.randomId(),
      NamespaceWithWorkflow.load(wdl.replaceAll("RUNTIME", runtime)),
      workflowDeclarations ++ inputs,
      options
    )
  }

  def buildEvaluatorBuilder(call: Call,
                            symbolsMap: Map[LocallyQualifiedName, WdlValue])
                           (engineFunctions: WdlFunctions[WdlValue],
                     preValueMapper: StringMapper = identity,
                     postValueMapper: WdlValueMapper = identity) = {

    val lookup = WdlExpression.standardLookupFunction(symbolsMap, call.task.declarations, engineFunctions) compose preValueMapper
    new WdlEvaluator(lookup, engineFunctions, preValueMapper, postValueMapper)
  }

  def localBackend(jobDescriptor: BackendJobDescriptor, conf: BackendConfigurationDescriptor) = {
    TestActorRef(new LocalJobExecutionActor(jobDescriptor, conf)).underlyingActor
  }

  def resolveCallDeclarations(call: Call, symbols: Map[String, WdlValue]): Seq[ResolvedDeclaration] = {
    call.task.declarations collect {
      case decl if decl.expression.isDefined => decl.resolveWith(decl.expression.get)
      case decl if call.inputMappings.contains(decl.name) => decl.resolveWith(call.inputMappings(decl.name))
      case decl if symbols.contains(decl.name) => decl.resolveWith(symbols(decl.name))
    }
  }

  def symbolsMapFor(call: Call, workflowDescriptor: BackendWorkflowDescriptor) = {
    val unqualifiedWorkflowInputs = workflowDescriptor.inputs map { case (k, v) => k.unqualified -> v }
    unqualifiedWorkflowInputs ++ inputsFor(workflowDescriptor, call)
  }

  def jobDescriptorFromSingleCallWorkflow(workflowDescriptor: BackendWorkflowDescriptor,
                                          symbolsMap: Map[String, WdlValue] = Map.empty) = {
    val call = workflowDescriptor.workflowNamespace.workflow.calls.head
    val jobKey = new BackendJobDescriptorKey(call, None, 1)
    val fullSymbolsMap = symbolsMap ++ symbolsMapFor(call, workflowDescriptor)
    val evaluatorBuilder = new WdlEvaluatorBuilder(buildEvaluatorBuilder(call, fullSymbolsMap))
    val callDeclarations = resolveCallDeclarations(call, fullSymbolsMap)
    new BackendJobDescriptor(workflowDescriptor, jobKey, evaluatorBuilder, callDeclarations)
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
