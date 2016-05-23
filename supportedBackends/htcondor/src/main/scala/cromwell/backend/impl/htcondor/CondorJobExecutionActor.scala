package cromwell.backend.impl.htcondor

import java.nio.file.attribute.PosixFilePermission
import java.nio.file.{FileSystems, Path}
import java.util.regex.Pattern

import akka.actor.Props
import cromwell.backend.BackendJobExecutionActor.{BackendJobExecutionResponse, FailedNonRetryableResponse, SucceededResponse}
import cromwell.backend._
import wdl4s.util.TryUtil

import scala.concurrent.Future
import scala.sys.process.ProcessLogger
import scala.util.{Failure, Success}

object CondorJobExecutionActor {

  val fileSystems = List(FileSystems.getDefault)

  def props(jobDescriptor: BackendJobDescriptor, configurationDescriptor: BackendConfigurationDescriptor): Props =
    Props(new CondorJobExecutionActor(jobDescriptor, configurationDescriptor))

}

class CondorJobExecutionActor(override val jobDescriptor: BackendJobDescriptor,
                              override val configurationDescriptor: BackendConfigurationDescriptor) extends BackendJobExecutionActor with SharedFileSystem {


  import CondorJobExecutionActor._
  import better.files._
  import cromwell.core.PathFactory._

  private val tag = s"CondorJobExecutionActor-${jobDescriptor.call.fullyQualifiedName}"

  lazy val cmds = new HtCondorCommands
  lazy val extProcess = new HtCondorProcess
  // stdout stderr writers for submit file logs
  private lazy val stdoutWriter = extProcess.untailedWriter(jobPaths.submitFileStdout)
  private lazy val stderrWriter = extProcess.tailedWriter(100, jobPaths.submitFileStderr)
  private val fileSystemsConfig = configurationDescriptor.backendConfig.getConfig("filesystems")
  override val sharedFsConfig = fileSystemsConfig.getConfig("local")
  private val workflowDescriptor = jobDescriptor.descriptor
  private val jobPaths = new JobPaths(workflowDescriptor, configurationDescriptor.backendConfig, jobDescriptor.key)
  // Files
  private val executionDir = jobPaths.callRoot
  private val returnCodePath = jobPaths.returnCode
  private val stdoutPath = jobPaths.stdout
  private val stderrPath = jobPaths.stderr
  private val scriptPath = jobPaths.script
  private val submitPath = jobPaths.submitFile

  val call = jobDescriptor.key.call
  val callEngineFunction = CondorJobExpressionFunctions(jobPaths)

  val lookup = jobDescriptor.inputs.apply _

  val runtimeAttributes = {
    val evaluateAttrs = call.task.runtimeAttributes.attrs mapValues (_.evaluate(lookup, callEngineFunction))
    // Fail the call if runtime attributes can't be evaluated
    val runtimeMap = TryUtil.sequenceMap(evaluateAttrs, "Runtime attributes evaluation").get
    CondorRuntimeAttributes(runtimeMap)
  }

  /**
    * Restart or resume a previously-started job.
    */
  override def recover: Future[BackendJobExecutionResponse] = {
    log.warning("{} HtCondor backend currently doesn't support recovering jobs. Starting {} again.", tag, jobDescriptor.key.call.fullyQualifiedName)
    Future(executeTask())
  }

  /**
    * Execute a new job.
    */
  override def execute: Future[BackendJobExecutionResponse] = Future(executeTask())

  private def executeTask(): BackendJobExecutionResponse = {
    val argv = Seq(HtCondorCommands.condor_submit, submitPath.toString)
    val process = extProcess.externalProcess(argv, ProcessLogger(stdoutWriter.writeWithNewline, stderrWriter.writeWithNewline))
    val condorReturnCode = process.exitValue() // blocks until process (i.e. condor submission) finishes
    log.debug("{} Return code of condor submit command: {}", tag, condorReturnCode)

    List(stdoutWriter.writer, stderrWriter.writer).foreach(_.flushAndClose())

    condorReturnCode match {
      case 0 if jobPaths.submitFileStderr.lines.toList.isEmpty =>
        log.info("{} {} submitted to HtCondor. Waiting for the job to complete via. RC file status.", tag, jobDescriptor.call.fullyQualifiedName)
        val pattern = Pattern.compile(HtCondorCommands.submit_output_pattern)
        //Number of lines in stdout for submit job will be 3 at max therefore reading all lines at once.
        log.debug(s"{} Output of submit process : {}", tag, jobPaths.submitFileStdout.lines.toList)
        val line = jobPaths.submitFileStdout.lines.toList.last
        val matcher = pattern.matcher(line)
        if (!matcher.matches())
          FailedNonRetryableResponse(jobDescriptor.key,
            new IllegalStateException("Failed to retrive jobs(id) and cluster id"), Option(condorReturnCode))
        else {
          val jobId = matcher.group(1).toInt
          val clusterId = matcher.group(2).toInt
          // TODO: `jobId` and `clusterId` is not used currently. But it's one of the fields that ought to be published in the metadata.
          log.info("{} {} mapped to HtCondor JobID: {} and ClusterId: {}", tag, jobDescriptor.call.fullyQualifiedName, jobId, clusterId)
          trackTaskToCompletion()
        }
      case 0 =>
        log.error(s"Unexpected! Recieved return code for condor submission as 0, although stderr file is non-empty: {}", jobPaths.submitFileStderr.lines)
        FailedNonRetryableResponse(jobDescriptor.key,
          new IllegalStateException(s"Execution process failed. HtCondor returned zero status code but non empty stderr file: $condorReturnCode"), Option(condorReturnCode))
      case nonZeroExitCode: Int =>
        FailedNonRetryableResponse(jobDescriptor.key,
          new IllegalStateException(s"Execution process failed. HtCondor returned non zero status code: $condorReturnCode"), Option(condorReturnCode))
    }
  }

  private def trackTaskToCompletion(): BackendJobExecutionResponse = {
    val processReturnCode = extProcess.jobReturnCode(returnCodePath) // Blocks until process completes
    log.debug("Process complete. RC file now exists with value: {}", processReturnCode)

    // TODO: Besides return code, do we also need to check based on stderr file?
    processReturnCode match {
      case rc: Int if rc == 0 | runtimeAttributes.continueOnReturnCode.continueFor(rc) => processSuccess(rc)
      case _ =>
        FailedNonRetryableResponse(jobDescriptor.key,
          new IllegalStateException("RC file contains non zero process return code."), Option(processReturnCode))
    }
  }

  private def processSuccess(rc: Int) = {
    processOutputs(callEngineFunction, jobPaths) match {
      case Success(outputs) => SucceededResponse(jobDescriptor.key, outputs)
      case Failure(e) =>
        val message = Option(e.getMessage) map {
          ": " + _
        } getOrElse ""
        FailedNonRetryableResponse(jobDescriptor.key, new Throwable("Failed post processing of outputs" + message, e), Option(rc))
    }
  }

  /**
    * Abort a running job.
    */
  override def abortJob(): Unit = Future.failed(new UnsupportedOperationException("HtCondorBackend currently doesn't support aborting jobs."))

  override def preStart(): Unit = {
    log.debug("{} Creating execution folder: {}", tag, executionDir)
    executionDir.toString.toFile.createIfNotExists(true)
    try {
      val localizedInputs = localizeInputs(jobPaths, false, fileSystems, jobDescriptor.inputs)
      val command = call.task.instantiateCommand(localizedInputs, callEngineFunction, identity).get
      log.debug("{} Creating bash script for executing command: {}", tag, command)
      writeScript(command, scriptPath, executionDir) // Writes the bash script for executing the command
      scriptPath.addPermission(PosixFilePermission.OWNER_EXECUTE) // Add executable permissions to the script.
      //TODO: Need to append other runtime attributes from Wdl to Condor submit file
      val attributes = Map(HtCondorRuntimeKeys.Executable -> scriptPath.toString,
          HtCondorRuntimeKeys.Output -> stdoutPath.toString,
          HtCondorRuntimeKeys.Error -> stderrPath.toString,
          HtCondorRuntimeKeys.Log -> jobPaths.htcondorLog.toString
        )
      cmds.generateSubmitFile(submitPath, attributes) // This writes the condor submit file
    } catch {
      case ex: Exception =>
        log.error(ex, "Failed to prepare task: " + ex.getMessage)
        throw ex
    }
  }

  /**
    * Writes the script file containing the user's command from the WDL as well
    * as some extra shell code for monitoring jobs
    */
  private def writeScript(instantiatedCommand: String, filePath: Path, containerRoot: Path) = {
    filePath.write(
      s"""#!/bin/sh
          |cd $containerRoot
          |$instantiatedCommand
          |echo $$? > rc
          |""".stripMargin)
  }
}
