package cromwell.backend.impl.htcondor


import java.nio.file.{Files, Path}

import com.typesafe.scalalogging.StrictLogging
import cromwell.core.{UntailedWriter, TailedWriter}
import scala.annotation.tailrec
import scala.sys.process._
import better.files._
import scala.language.postfixOps
import cromwell.core.PathFactory.{EnhancedPath, FlushingAndClosingWriter}

case class CondorJobId(jobId: Int, clusterId: Int)

case class CondorJob(condorJobId: CondorJobId)

case class CondorStatus(condorJobId: CondorJobId, status: Int)

sealed trait JobStatus
sealed trait TerminalJobStatus extends JobStatus
case object Created extends JobStatus
case object Running extends JobStatus
case object Succeeded extends TerminalJobStatus
case object Failed extends TerminalJobStatus
case object Cancelled extends TerminalJobStatus

object HtCondorCommands {
  val submit_output_pattern = "(\\d*) job\\(s\\) submitted to cluster (\\d*)\\."
  val condor_submit = "condor_submit"
  val condor_queue = "condor_q"
  val condor_remove = "condor_rm"
}

class HtCondorCommands extends StrictLogging {

  def generateSubmitFile(path: Path, attributes: Map[String, String]): String = {
    def htCondorSubmitCommand(filePath: Path) = {
      s"${HtCondorCommands.condor_submit} ${filePath.toString}"
    }

    val submitFileWriter = path.untailed
    attributes.foreach(attribute => submitFileWriter.writeWithNewline(s"${attribute._1}=${attribute._2}"))
    submitFileWriter.writeWithNewline(HtCondorRuntimeKeys.Queue)
    submitFileWriter.writer.flushAndClose()
    logger.debug(s"submit file name is : $path")
    logger.debug(s"content of file is : ${path.lines.toList}")
    htCondorSubmitCommand(path)
  }

  def statusCommand(): String = s"${HtCondorCommands.condor_queue} -xml"
}

class HtCondorProcess {
  val stdout = new StringBuilder
  val stderr = new StringBuilder

  def processLogger: ProcessLogger = ProcessLogger(stdout append _, stderr append _)
  def processStdout: String = stdout.toString().trim
  def processStderr: String = stderr.toString().trim
  def commandList(command: String): Seq[String] = Seq("/bin/bash",command)
  def untailedWriter(path: Path): UntailedWriter = path.untailed
  def tailedWriter(limit: Int, path: Path): TailedWriter = path.tailed(limit)
  def externalProcess(cmdList: Seq[String], processLogger: ProcessLogger = processLogger): Process = cmdList.run(processLogger)

  /**
    * Returns the RC of this job when it finishes.  Sleeps and polls
    * until the 'rc' file is generated
    */
  def jobReturnCode(returnCodeFilePath: Path): Int = {

    @tailrec
    def recursiveWait(): Int = Files.exists(returnCodeFilePath) match {
      case true => returnCodeFilePath.contentAsString.stripLineEnd.toInt
      case false =>
        Thread.sleep(5000)
        recursiveWait()
    }

    recursiveWait()
  }

}

object HtCondorRuntimeKeys {
  val Executable = "executable"
  val Arguments = "arguments"
  val Error = "error"
  val Output = "output"
  val Log = "log"
  val Queue = "queue"
  val Rank = "rank"
  val Requirements = "requirements"
  val RequestMemory = "request_memory"
  val Cpu = "request_cpus"
  val Disk = "request_disk"
}
