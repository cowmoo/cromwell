package cromwell.database.slick

import java.sql.Timestamp

import cromwell.database.obj.WorkflowMetadataSummary

trait WorkflowMetadataSummaryComponent {

  this: DriverComponent =>

  import driver.api._

  class WorkflowMetadataSummaries(tag: Tag) extends Table[WorkflowMetadataSummary](tag, "WORKFLOW_METADATA_SUMMARY") {
    def workflowMetadataSummaryId = column[Int]("WORKFLOW_METADATA_SUMMARY_ID", O.PrimaryKey, O.AutoInc)
    def workflowExecutionUuid = column[String]("WORKFLOW_EXECUTION_UUID")
    def name = column[Option[String]]("WORKFLOW_NAME")
    def status = column[Option[String]]("WORKFLOW_STATUS")
    def startDate = column[Option[Timestamp]]("START_DT")
    def endDate = column[Option[Timestamp]]("END_DT")

    override def * = (workflowExecutionUuid, name, status, startDate, endDate, workflowMetadataSummaryId.?) <>
      (WorkflowMetadataSummary.tupled, WorkflowMetadataSummary.unapply)

    def uuidIndex = index("WORKFLOW_METADATA_UUID_IDX", workflowExecutionUuid, unique = true)

    def nameIndex = index("WORKFLOW_METADATA_NAME_IDX", name, unique = false)

    def statusIndex = index("WORKFLOW_METADATA_STATUS_IDX", status, unique = false)
  }

  val workflowMetadataSummaries = TableQuery[WorkflowMetadataSummaries]

  val workflowMetadataSummaryAutoInc = workflowMetadataSummaries returning workflowMetadataSummaries.map(_.workflowMetadataSummaryId)

  val workflowMetadataSummariesByUuid = Compiled(
    (workflowUuid: Rep[String]) => for {
      workflowMetadataSummary <- workflowMetadataSummaries
      if workflowMetadataSummary.workflowExecutionUuid === workflowUuid
    } yield workflowMetadataSummary)
}
