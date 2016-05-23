package cromwell.engine

sealed trait WorkflowState {
  def isTerminal: Boolean
  def takesPrecedence(that: WorkflowState): WorkflowState
}

object WorkflowState {
  private lazy val WorkflowState = Seq(WorkflowSubmitted, WorkflowRunning, WorkflowFailed, WorkflowSucceeded, WorkflowAborting, WorkflowAborted)

  def fromString(str: String): WorkflowState = WorkflowState.find(_.toString == str).getOrElse(
    throw new NoSuchElementException(s"No such WorkflowState: $str"))

  def takesPrecedence(thiz: String, that: String): WorkflowState = {
    val thisState = fromString(thiz)
    val thatState = fromString(that)
    thisState.takesPrecedence(thatState)
  }

  def StatusConflictResolutionFn(thiz: String, that: String) = takesPrecedence(thiz, that).toString
}

case object WorkflowSubmitted extends WorkflowState {
  override val toString: String = "Submitted"
  override val isTerminal = false
  override def takesPrecedence(that: WorkflowState): WorkflowState = that
}

case object WorkflowRunning extends WorkflowState {
  override val toString: String = "Running"
  override val isTerminal = false
  override def takesPrecedence(that: WorkflowState): WorkflowState = if (that == WorkflowSubmitted) this else that
}

case object WorkflowAborting extends WorkflowState {
  override val toString: String = "Aborting"
  override val isTerminal = false
  override def takesPrecedence(that: WorkflowState): WorkflowState = if (!that.isTerminal) that else this
}

case object WorkflowFailed extends WorkflowState {
  override val toString: String = "Failed"
  override val isTerminal = true
  override def takesPrecedence(that: WorkflowState): WorkflowState = this
}

case object WorkflowSucceeded extends WorkflowState {
  override val toString: String = "Succeeded"
  override val isTerminal = true
  override def takesPrecedence(that: WorkflowState): WorkflowState = if (that == WorkflowFailed) that else this
}

case object WorkflowAborted extends WorkflowState {
  override val toString: String = "Aborted"
  override val isTerminal = true
  override def takesPrecedence(that: WorkflowState): WorkflowState = if (that.isTerminal) that else this
}