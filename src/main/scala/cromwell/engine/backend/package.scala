package cromwell.engine

import scala.concurrent.{ExecutionContext, Future}

package object backend {

  trait JobKey

  implicit class EnhancedFutureFuture[A](val ffa: Future[Future[A]])(implicit ec: ExecutionContext) {
    def flatten: Future[A] = ffa flatMap { fa => fa }
  }

  implicit class EnhancedExecutionResult(val result: ExecutionResult) extends AnyVal {
    def future = Future.successful(result)
  }
}
