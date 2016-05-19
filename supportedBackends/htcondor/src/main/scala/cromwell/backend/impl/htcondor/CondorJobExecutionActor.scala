package cromwell.backend.impl.htcondor

import java.nio.file.{Files, Path, FileSystems}
import java.util.regex.Pattern

import akka.actor.Props
import cromwell.backend.BackendJobExecutionActor.{SucceededResponse, FailedNonRetryableResponse, BackendJobExecutionResponse}
import cromwell.backend._
import wdl4s.util.TryUtil

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.sys.process.ProcessLogger
import scala.util.{Failure, Success, Try}

object CondorJobExecutionActor {

  val fileSystems = List(FileSystems.getDefault)

  def props(jobDescriptor: BackendJobDescriptor, configurationDescriptor: BackendConfigurationDescriptor): Props =
    Props(new CondorJobExecutionActor(jobDescriptor, configurationDescriptor))

}

class CondorJobExecutionActor(override val jobDescriptor: BackendJobDescriptor,
                              override val configurationDescriptor: BackendConfigurationDescriptor) extends BackendJobExecutionActor with SharedFileSystem {


  import CondorJobExecutionActor._
  import cromwell.core.PathFactory._
  import better.files._

  lazy val cmds = new HtCondorCommands {}
  lazy val extProcess = new HtCondorProcess {}
  lazy val parser = new HtCondorClassAdParser {}

  val fileSystemsConfig = configurationDescriptor.backendConfig.getConfig("filesystems")
  override val sharedFsConfig = fileSystemsConfig.getConfig("local")

  val workflowDescriptor = jobDescriptor.descriptor
  val jobPaths = new JobPaths(workflowDescriptor, configurationDescriptor.backendConfig, jobDescriptor.key)

  // Files
  val executionDir = jobPaths.callRoot
  val returnCodePath = jobPaths.returnCode
  val stdoutPath = jobPaths.stdout
  val stderrPath = jobPaths.stderr
  val scriptPath = jobPaths.script
  val submitPath = jobPaths.submitFile

  // stdout stderr writers for submit file logs
  lazy val stdoutWriter = extProcess.getUntailedWriter(jobPaths.submitFileStdout)
  lazy val stderrWriter = extProcess.getTailedWriter(100, jobPaths.submitFileStderr)
  val argv = extProcess.getCommandList(scriptPath.toString)

  val call = jobDescriptor.key.call
  val callEngineFunction = CondorJobExpressionFunctions(jobPaths)

  val lookup = jobDescriptor.inputs.apply _

  val runtimeAttributes = {
    val evaluateAttrs = call.task.runtimeAttributes.attrs mapValues (_.evaluate(lookup, callEngineFunction))
    // Fail the call if runtime attributes can't be evaluated
    TryUtil.sequenceMap(evaluateAttrs, "Runtime attributes evaluation").get.mapValues(_.valueString)
  }

  /**
    * Restart or resume a previously-started job.
    */
  override def recover: Future[BackendJobExecutionResponse] = {
    log.warning(s"HtCondor backend currently doesn't support recovering jobs. Starting ${jobDescriptor.key.call.fullyQualifiedName} again.")
    Future(executeTask())
  }

  /**
    * Execute a new job.
    */
  override def execute: Future[BackendJobExecutionResponse] = Future(executeTask())

  /**
    * Abort a running job.
    */
  override def abortJob(): Unit = Future.failed(new UnsupportedOperationException("HtCondorBackend currently doesn't support aborting jobs."))

  override def preStart(): Unit = {
    log.debug(s"Creating execution folder: $executionDir")
    executionDir.toString.toFile.createIfNotExists(true)
    try {
      val localizedInputs = localizeInputs(jobPaths, false, fileSystems, jobDescriptor.inputs)
      val command = call.task.instantiateCommand(localizedInputs, callEngineFunction, identity).get
      log.debug(s"Creating bash script for executing command: $command.")
      writeScript(command, scriptPath, executionDir) // Writes the bash script for executing the command
      //TODO: need to access other requirements for submit file from runtime requirements
      val attributes = Map(HtCondorRuntimeKeys.executable -> scriptPath.toString,
        HtCondorRuntimeKeys.output -> stdoutPath.toString,
        HtCondorRuntimeKeys.error -> stderrPath.toString)
      val condorSubmitFile = cmds.submitCommand(submitPath, attributes)
//      writeScript(condorSubmitFile, submitPath, executionDir)
    } catch {
      case ex: Exception =>
        log.error(ex, s"Failed to prepare task: ${ex.getMessage}")
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

  private def executeTask(): BackendJobExecutionResponse = {
    val newArgv: scala.Seq[String] = Seq(HtCondorCommands.condor_submit, submitPath.toString)
    val process = extProcess.externalProcess(newArgv, ProcessLogger(stdoutWriter.writeWithNewline, stderrWriter.writeWithNewline))
    val processReturnCode = process.exitValue() // blocks until process (i.e. condor submission) finishes
    log.debug(s"processReturnCode  : $processReturnCode")

    List(stdoutWriter.writer, stderrWriter.writer).foreach(_.flushAndClose())
    log.debug(s"done flushing")
    val stderrFileLength = Try(Files.size(jobPaths.submitFileStderr)).getOrElse(0L)
    log.debug(s"stderr file length : $stderrFileLength ")

    if (processReturnCode != 0) {
      FailedNonRetryableResponse(jobDescriptor.key,
        new IllegalStateException(s"Execution process failed. Process return code non zero: $processReturnCode"), Option(processReturnCode))
    } else if (stderrFileLength > 0) {
      FailedNonRetryableResponse(jobDescriptor.key,
        new IllegalStateException("StdErr file is not empty"), None)
    } else {
      log.debug(s"Parse stdout output file")
      val pattern = Pattern.compile(HtCondorCommands.submit_output_pattern)
      //Number of lines in stdout for submit job will be 3 at max therefore reading all lines at once.
      log.debug(s"output of submit process : ${stdoutPath.lines.toList}")
      val line = stdoutPath.lines.toList.last
      val matcher = pattern.matcher(line)
      log.debug(s"submit process stdout last line : $line")
      if (!matcher.matches())
        FailedNonRetryableResponse(jobDescriptor.key,
          new IllegalStateException("HtCondor: Failed to retrive jobs(id) and cluster id"), Option(processReturnCode))
      val jobId = matcher.group(1).toInt
      val clusterId = matcher.group(2).toInt
      log.debug(s"task job is : $jobId and cluster id is : $clusterId")
      trackTaskStatus(CondorJob(CondorJobId(jobId, clusterId)))
    }
  }

  @tailrec
  private def trackTaskStatus(job: CondorJob): BackendJobExecutionResponse = {
    val process = extProcess.externalProcess(extProcess.getCommandList(cmds.statusCommand()))
    val processReturnCode = process.exitValue() // blocks until process finishes
    log.debug(s"trackTaskStatus -> processReturnCode : $processReturnCode and stderr : ${extProcess.getProcessStderr().length}")

    if (processReturnCode != 0 || extProcess.getProcessStderr().nonEmpty)
      FailedNonRetryableResponse(jobDescriptor.key,
        new IllegalStateException("StdErr file is not empty"), Option(processReturnCode))
    else {
      val status = parser.getJobStatus(extProcess.getProcessStdout(), job.condorJobId)
      if (status == Succeeded)
        processSuccess(processReturnCode)
      else if (status == Failed)
        FailedNonRetryableResponse(jobDescriptor.key, new IllegalStateException("Job Final Status: Failed"), Option(processReturnCode))
      else {
        Thread.sleep(5000) // Wait for 5 seconds
        trackTaskStatus(job)
      }
    }

  }

  private def processSuccess(rc: Int) = {
    processOutputs(callEngineFunction, jobPaths) match {
      case Success(outputs) => SucceededResponse(jobDescriptor.key, outputs)
      case Failure(e) =>
        val message = Option(e.getMessage) map { ": " + _ } getOrElse ""
        FailedNonRetryableResponse(jobDescriptor.key, new Throwable("Failed post processing of outputs" + message, e), Option(rc))
    }
  }

}
