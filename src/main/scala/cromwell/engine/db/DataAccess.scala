package cromwell.engine.db

import cromwell.engine.ExecutionStatus.ExecutionStatus
import cromwell.engine.db.DataAccess.StdoutAndStderr
import cromwell.engine.{WorkflowOutputs, _}
import cromwell.engine.backend.JobKey
import cromwell.engine.db.slick._
import cromwell.engine.workflow.{BackendCallKey, ExecutionStoreKey, OutputKey}
import cromwell.webservice.{CallCachingParameters, WorkflowQueryParameters, WorkflowQueryResponse}
import wdl4s._

import scala.concurrent.Future
import scala.language.postfixOps

object DataAccess {
  val globalDataAccess: DataAccess = new slick.SlickDataAccess()
  case class StdoutAndStderr(stdout: String, stderr: String)
}

trait DataAccess {
  def standardOutAndError(descriptor: WorkflowDescriptor, callName: String, callLogKeys: Seq[ExecutionDatabaseKey]): Future[Traversable[StdoutAndStderr]] = ???

  /**
   * Creates a row in each of the backend-info specific tables for each call in `calls` corresponding to the backend
   * `backend`.  Or perhaps defer this?
   */
  def createWorkflow(workflowDescriptor: WorkflowDescriptor,
                     workflowInputs: Traversable[SymbolStoreEntry],
                     calls: Traversable[Scope]): Future[Unit]

  def getWorkflowState(workflowId: WorkflowId): Future[Option[WorkflowState]]

  def getWorkflow(workflowId: WorkflowId): Future[WorkflowDescriptor]

  def getWorkflow(workflowExecutionId: Int): Future[WorkflowDescriptor]

  def getWorkflowsByState(states: Traversable[WorkflowState]): Future[Traversable[WorkflowDescriptor]]

  def getExecutionBackendInfo(workflowId: WorkflowId, call: Call): Future[Map[String, Option[String]]]

  def updateExecutionInfo(workflowId: WorkflowId, callKey: BackendCallKey, key: String, value: Option[String]): Future[Unit]

  def updateWorkflowState(workflowId: WorkflowId, workflowState: WorkflowState): Future[Unit]

  def getAllSymbolStoreEntries(workflowId: WorkflowId): Future[Traversable[SymbolStoreEntry]]

  // TODO needed to support compatibility with current code, this seems like an inefficient way of getting
  // TODO workflow outputs.
  /** Returns all outputs for this workflowId */
  def getWorkflowOutputs(workflowId: WorkflowId): Future[Traversable[SymbolStoreEntry]]

  def getAllOutputs(workflowId: WorkflowId): Future[Traversable[SymbolStoreEntry]]

  def getAllInputs(workflowId: WorkflowId): Future[Traversable[SymbolStoreEntry]]

  /** Get all outputs for the scope of this call. */
  def getOutputs(workflowId: WorkflowId, key: ExecutionDatabaseKey): Future[Traversable[SymbolStoreEntry]]

  /** Get all inputs for the scope of this call. */
  def getInputs(id: WorkflowId, call: Call): Future[Traversable[SymbolStoreEntry]]

  /** Should fail if a value is already set.  The keys in the Map are locally qualified names. */
  def setOutputs(workflowId: WorkflowId, key: OutputKey, callOutputs: WorkflowOutputs, workflowOutputFqns: Seq[ReportableSymbol]): Future[Unit]

  /** Updates the existing input symbols to replace expressions with real values **/
  def updateCallInputs(workflowId: WorkflowId, key: BackendCallKey, callInputs: CallInputs): Future[Int]

  def setExecutionEvents(workflowId: WorkflowId, callFqn: String, shardIndex: Option[Int], events: Seq[ExecutionEventEntry]): Future[Unit]

  /** Gets a mapping from call FQN to an execution event entry list */
  def getAllExecutionEvents(workflowId: WorkflowId): Future[Map[ExecutionDatabaseKey, Seq[ExecutionEventEntry]]]

  def setStatus(workflowId: WorkflowId, keys: Traversable[ExecutionDatabaseKey], executionStatus: ExecutionStatus): Future[Unit] = {
    setStatus(workflowId, keys, CallStatus(executionStatus, None, None, None))
  }

  def setStatus(workflowId: WorkflowId, keys: Traversable[ExecutionDatabaseKey], callStatus: CallStatus): Future[Unit]

  def getExecutionStatuses(workflowId: WorkflowId): Future[Map[ExecutionDatabaseKey, CallStatus]]

  /** Return all execution entries for the FQN, including collector and shards if any */
  def getExecutionStatuses(workflowId: WorkflowId, fqn: FullyQualifiedName): Future[Map[ExecutionDatabaseKey, CallStatus]]

  def getExecutionStatus(workflowId: WorkflowId, key: ExecutionDatabaseKey): Future[Option[CallStatus]]

  def insertCalls(workflowId: WorkflowId, keys: Traversable[ExecutionStoreKey]): Future[Unit]

  /** Shutdown. NOTE: Should (internally or explicitly) use AsyncExecutor.shutdownExecutor.
    * TODO this is only called from a test. */
  def shutdown(): Future[Unit]

  def getExecutions(id: WorkflowId): Future[Traversable[Execution]]

  def getExecutionsForRestart(id: WorkflowId): Future[Traversable[Execution]]

  def getExecutionsWithResuableResultsByHash(hash: String): Future[Traversable[Execution]]

  /** Fetch the workflow having the specified `WorkflowId`. */
  def getWorkflowExecution(workflowId: WorkflowId): Future[WorkflowExecution]

  def getWorkflowExecutionAux(id: WorkflowId): Future[WorkflowExecutionAux]

  def updateWorkflowOptions(workflowId: WorkflowId, workflowOptionsJson: String): Future[Unit]

  def resetNonResumableExecutions(workflowId: WorkflowId, isResumable: ExecutionAndExecutionInfo => Boolean): Future[Unit]

  def findResumableExecutions(workflowId: WorkflowId,
                              isResumable: ExecutionAndExecutionInfo => Boolean,
                              jobKeyBuilder: ExecutionAndExecutionInfo => JobKey): Future[Map[ExecutionDatabaseKey, JobKey]]

  def queryWorkflows(queryParameters: WorkflowQueryParameters): Future[WorkflowQueryResponse]

  def updateCallCaching(cachingParameters: CallCachingParameters): Future[Int]

  def infosByExecution(id: WorkflowId): Future[Traversable[ExecutionInfosByExecution]]
}
