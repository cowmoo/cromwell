package cromwell.engine.backend.local

import better.files._
import cromwell.engine.backend.{BackendCall, LocalFileSystemBackendCall, _}
import cromwell.engine.workflow.BackendCallKey
import cromwell.engine.{AbortRegistrationFunction, CallContext, WorkflowDescriptor}
import wdl4s.CallInputs
import wdl4s.values.WdlValue

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

case class LocalBackendCall(backend: LocalBackend,
                            jobDescriptor: BackendCallJobDescriptor,
                            callAbortRegistrationFunction: Option[AbortRegistrationFunction]) extends BackendCall with LocalFileSystemBackendCall {
  val dockerContainerExecutionDir = workflowDescriptor.workflowRootPathWithBaseRoot(LocalBackend.ContainerRoot)
  lazy val containerCallRoot = runtimeAttributes.docker match {
    case Some(docker) => callRootPathWithBaseRoot(LocalBackend.ContainerRoot)
    case None => callRootPath
  }
  val returnCode = callRootPath.resolve("rc")
  val stdout = callRootPath.resolve("stdout")
  val stderr = callRootPath.resolve("stderr")
  val script = callRootPath.resolve("script")
  private val callContext = new CallContext(callRootPath.fullPath, stdout.fullPath, stderr.fullPath)
  val callEngineFunctions = new LocalCallEngineFunctions(workflowDescriptor.ioManager, callContext)

  callRootPath.toFile.mkdirs

  def instantiateCommand: Try[String] = {
    val backendInputs = backend.adjustInputPaths(this)
    val pathTransformFunction: WdlValue => WdlValue = runtimeAttributes.docker match {
      case Some(_) => backend.toDockerPath
      case None => (v: WdlValue) => v
    }
    call.instantiateCommandLine(backendInputs, callEngineFunctions, pathTransformFunction)
  }

  override def execute(implicit ec: ExecutionContext) = backend.execute(this)

  override def poll(previous: ExecutionHandle)(implicit ec: ExecutionContext) = Future.successful(previous)

  override def useCachedCall(avoidedTo: BackendCall)(implicit ec: ExecutionContext): Future[ExecutionHandle] =
    backend.useCachedCall(avoidedTo.asInstanceOf[LocalBackendCall], this)

  override def stdoutStderr: CallLogs = backend.stdoutStderr(this)
}
