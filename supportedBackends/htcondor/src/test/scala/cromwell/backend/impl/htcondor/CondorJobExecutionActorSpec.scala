package cromwell.backend.impl.htcondor

import java.io.Writer
import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import better.files._
import com.typesafe.config.ConfigFactory
import cromwell.backend.BackendJobExecutionActor.{FailedNonRetryableResponse, SucceededResponse}
import cromwell.backend.{BackendConfigurationDescriptor, BackendJobDescriptor, BackendJobDescriptorKey, BackendWorkflowDescriptor}
import cromwell.core._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import spray.json.{JsObject, JsValue}
import wdl4s._
import wdl4s.values.WdlValue

import scala.sys.process.{Process, ProcessLogger}


class CondorJobExecutionActorSpec extends TestKit(ActorSystem("CondorJobExecutionActorSpec"))
  with WordSpecLike
  with Matchers
  with MockitoSugar
  with BeforeAndAfterAll
  with ImplicitSender {

  private val htCondorParser: HtCondorClassAdParser = new HtCondorClassAdParser {}
  private val htCondorCommands: HtCondorCommands = new HtCondorCommands {}
  private val htCondorProcess: HtCondorProcess = mock[HtCondorProcess]
  private val stdoutResult =
    s"""<?xml version="1.0"?>
       <!DOCTYPE classads SYSTEM "classads.dtd">
       <classads>
       <c>
       <a n="ProcId"><i>2</i></a>
       <a n="EnteredCurrentStatus"><i>1454439245</i></a>
       <a n="ClusterId"><i>88</i></a>
       <a n="JobStatus"><i>4</i></a>
       <a n="MachineAttrSlotWeight0"><i>8</i></a>
       </c>
       </classads>
     """.stripMargin

  private val helloWorldWdl =
    """
      |task hello {
      |  command {
      |    echo "Hello World!"
      |  }
      |  output {
      |    String salutation = read_string(stdout())
      |  }
      |  RUNTIME
      |}
      |
      |workflow hello {
      |  call hello
      |}
    """.stripMargin

  private def buildWorkflowDescriptor(wdl: WdlSource,
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

  private def jobDescriptorFromSingleCallWorkflow(workflowDescriptor: BackendWorkflowDescriptor,
                                          inputs: Map[String, WdlValue] = Map.empty) = {
    val call = workflowDescriptor.workflowNamespace.workflow.calls.head
    val jobKey = new BackendJobDescriptorKey(call, None, 1)
    new BackendJobDescriptor(workflowDescriptor, jobKey, inputs)
  }

  private val backendConfig = ConfigFactory.parseString(
    s"""{
        |  root = "local-cromwell-executions"
        |  filesystems {
        |    local {
        |      localization = [
        |        "hard-link", "soft-link", "copy"
        |      ]
        |    }
        |  }
        |}
        """.stripMargin)

  private val backendWorkflowDescriptor = buildWorkflowDescriptor(helloWorldWdl)
  private val backendConfigurationDescriptor = BackendConfigurationDescriptor(backendConfig, ConfigFactory.load)

  private val jobDescriptor = jobDescriptorFromSingleCallWorkflow(backendWorkflowDescriptor)
  private val jobPaths = new JobPaths(backendWorkflowDescriptor, backendConfig, jobDescriptor.key)

  private val executionDir = jobPaths.callRoot

  private val stdout = Paths.get(executionDir.path.toString, "stdout")
  stdout.toString.toFile.createIfNotExists(false)
  stdout <<
    """Submitting job(s)..
      |2 job(s) submitted to cluster 88.
    """.stripMargin.trim

  private val stderr = Paths.get(executionDir.path.toString, "stderr")
  stderr.toString.toFile.createIfNotExists(false)

  private val rc = Paths.get(executionDir.path.toString, "rc")
  rc.toString.toFile.createIfNotExists(false)
  rc << s"""0""".stripMargin.trim

  trait MockWriter extends Writer {
    var closed = false
    override def close() = closed = true
    override def flush() = { }
    override def write(a: Array[Char], b: Int, c: Int) = {}
  }

  trait MockPathWriter extends PathWriter {
    override val path: Path = mock[Path]
    override lazy val writer : Writer = new MockWriter {}
  }

  "executeTask method" should {
    "return succeeded task status with stdout " in {
      val stubProcess = mock[Process]
      val stubUntailed = new UntailedWriter(stdout) with MockPathWriter
      val stubTailed = new TailedWriter(stderr, 100) with MockPathWriter
      val stderrResult = ""

      when(htCondorProcess.getCommandList(any[String])).thenReturn(Seq.empty[String])
      when(htCondorProcess.externalProcess(any[Seq[String]], any[ProcessLogger])).thenReturn(stubProcess)
      when(stubProcess.exitValue()).thenReturn(0)
      when(htCondorProcess.getTailedWriter(any[Int], any[Path])).thenReturn(stubTailed)
      when(htCondorProcess.getUntailedWriter(any[Path])).thenReturn(stubUntailed)
      when(htCondorProcess.getProcessStdout()).thenReturn(stdoutResult)
      when(htCondorProcess.getProcessStderr()).thenReturn(stderrResult)

      val backend = TestActorRef(new CondorJobExecutionActor(jobDescriptor, backendConfigurationDescriptor) {
        override lazy val cmds = htCondorCommands
        override lazy val extProcess = htCondorProcess
        override lazy val parser = htCondorParser
      }).underlyingActor

      whenReady(backend.execute) { response =>
        response shouldBe a[SucceededResponse]
        verify(htCondorProcess, times(2)).externalProcess(any[Seq[String]], any[ProcessLogger])
        verify(htCondorProcess, times(1)).getTailedWriter(any[Int], any[Path])
        verify(htCondorProcess, times(1)).getUntailedWriter(any[Path])
      }

    }
  }

  "executeTask method" should {
    "return failed task status with stderr " in {
      val backend = TestActorRef(new CondorJobExecutionActor(jobDescriptor, backendConfigurationDescriptor){
        override lazy val cmds = htCondorCommands
        override lazy val extProcess = htCondorProcess
        override lazy val parser = htCondorParser
      }).underlyingActor
      stderr <<
        s"""
           |test
         """.stripMargin
      val stubProcess = mock[Process]
      val stubUntailed = new UntailedWriter(stdout) with MockPathWriter
      val stubTailed = new TailedWriter(stderr, 100) with MockPathWriter
      val stderrResult = "failed"

      when(htCondorProcess.externalProcess(any[Seq[String]], any[ProcessLogger])).thenReturn(stubProcess)
      when(stubProcess.exitValue()).thenReturn(0)
      when(htCondorProcess.getTailedWriter(any[Int], any[Path])).thenReturn(stubTailed)
      when(htCondorProcess.getUntailedWriter(any[Path])).thenReturn(stubUntailed)
      when(htCondorProcess.getProcessStdout()).thenReturn(stdoutResult)
      when(htCondorProcess.getProcessStderr()).thenReturn(stderrResult)

      whenReady(backend.execute) { response =>
        response shouldBe a[FailedNonRetryableResponse]
        assert(response.asInstanceOf[FailedNonRetryableResponse].throwable.getMessage == "StdErr file is not empty")
      }
    }
  }

  "executeTask method" should {
    "return failed task status with stderr on non-zero process exit " in {
      val backend = TestActorRef(new CondorJobExecutionActor(jobDescriptor, backendConfigurationDescriptor){
        override lazy val cmds = htCondorCommands
        override lazy val extProcess = htCondorProcess
        override lazy val parser = htCondorParser
      }).underlyingActor
      val stubProcess = mock[Process]
      val stubUntailed = new UntailedWriter(stdout) with MockPathWriter
      val stubTailed = new TailedWriter(stderr, 100) with MockPathWriter
      val stderrResult = ""

      when(htCondorProcess.externalProcess(any[Seq[String]], any[ProcessLogger])).thenReturn(stubProcess)
      when(stubProcess.exitValue()).thenReturn(-1)
      when(htCondorProcess.getTailedWriter(any[Int], any[Path])).thenReturn(stubTailed)
      when(htCondorProcess.getUntailedWriter(any[Path])).thenReturn(stubUntailed)
      when(htCondorProcess.getProcessStdout()).thenReturn(stdoutResult)
      when(htCondorProcess.getProcessStderr()).thenReturn(stderrResult)

      whenReady(backend.execute) { response =>
        response shouldBe a[FailedNonRetryableResponse]
        assert(response.asInstanceOf[FailedNonRetryableResponse].throwable.getMessage == "Execution process failed. Process return code non zero: -1")
      }
    }
  }

  override def afterAll(): Unit = {
    stdout.delete()
    stderr.delete()
    rc.delete()
    executionDir.delete(true)
    system.shutdown()
  }
}