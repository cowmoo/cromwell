package cromwell.webservice

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{OneForOneStrategy, _}
import cromwell.webservice.PerRequest._
import spray.http.StatusCodes._
import spray.http._
import spray.httpx.marshalling.ToResponseMarshaller
import spray.routing.RequestContext

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * This actor controls the lifecycle of a request. It is responsible for forwarding the initial message
 * to a target handling actor. This actor waits for the target actor to signal completion (via a message),
 * timeout, or handle an exception. It is this actors responsibility to respond to the request and
 * shutdown itself and child actors.
 *
 * Request completion can be signaled in 2 ways:
 * 1) with just a response object
 * 2) with a RequestComplete message which can specify http status code as well as the response
 */
trait PerRequest extends Actor {
  import context._

  def r: RequestContext
  def target: ActorRef
  def message: AnyRef
  def timeout: Duration

  setReceiveTimeout(timeout)
  target ! message

  def receive = {
    // The [Any] type parameter appears to be required for version of Scala > 2.11.2,
    // the @ unchecked is required to muzzle erasure warnings.
    case message: RequestComplete[Any] @ unchecked => complete(message.response)(message.marshaller)
    case message: RequestCompleteWithHeaders[Any] @ unchecked => complete(message.response, message.headers:_*)(message.marshaller)
    case ReceiveTimeout => complete(GatewayTimeout)
    case x =>
      system.log.error("Unsupported response message sent to PreRequest actor: " + Option(x).getOrElse("null").toString)
      complete(InternalServerError)
  }

  /**
   * Complete the request sending the given response and status code
   * @param response to send to the caller
   * @param marshaller to use for marshalling the response
   * @tparam T the type of the response
   * @return
   */
  private def complete[T](response: T, headers: HttpHeader*)(implicit marshaller: ToResponseMarshaller[T]) = {
    val additionalHeaders = None
    r.withHttpResponseHeadersMapped(h => h ++ headers ++ additionalHeaders).complete(response)
    stop(self)
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case e =>
        system.log.error(e, "error processing request: " + r.request.uri)
        r.complete(InternalServerError, e.getMessage)
        Stop
    }
}

object PerRequest {
  sealed trait PerRequestMessage
  /**
   * Report complete, follows same pattern as spray.routing.RequestContext.complete; examples of how to call
   * that method should apply here too. E.g. even though this method has only one parameter, it can be called
   * with 2 where the first is a StatusCode: RequestComplete(StatusCode.Created, response)
   */
  case class RequestComplete[T](response: T)(implicit val marshaller: ToResponseMarshaller[T]) extends PerRequestMessage

  /**
   * Report complete with response headers. To response with a special status code the first parameter can be a
   * tuple where the first element is StatusCode: RequestCompleteWithHeaders((StatusCode.Created, results), header).
   * Note that this is here so that RequestComplete above can behave like spray.routing.RequestContext.complete.
   */
  case class RequestCompleteWithHeaders[T](response: T, headers: HttpHeader*)(implicit val marshaller: ToResponseMarshaller[T]) extends PerRequestMessage

  /** allows for pattern matching with extraction of marshaller */
  private object RequestComplete_ {
    def unapply[T](requestComplete: RequestComplete[T]) = Some((requestComplete.response, requestComplete.marshaller))
  }

  /** allows for pattern matching with extraction of marshaller */
  private object RequestCompleteWithHeaders_ {
    def unapply[T](requestComplete: RequestCompleteWithHeaders[T]) = Some((requestComplete.response, requestComplete.headers, requestComplete.marshaller))
  }

  case class WithProps(r: RequestContext, props: Props, message: AnyRef, timeout: Duration, name: String) extends PerRequest {
    lazy val target = context.actorOf(props, name)
  }
}

/**
 * Provides factory methods for creating per request actors
 */
trait PerRequestCreator {
  implicit def actorRefFactory: ActorRefFactory

  def perRequest(r: RequestContext,
                 props: Props, message: AnyRef,
                 timeout: Duration = 1 minutes,
                 name: String = PerRequestCreator.endpointActorName) = {
    actorRefFactory.actorOf(Props(new WithProps(r, props, message, timeout, name)), name)
  }
}

object PerRequestCreator {
  /*
    This is yucky. For lack of a better idea on how to name the individual endpoint actors I've shamelessly stolen
    what Agora is doing. I believe the Monsanto library will clean this up but we're not using it yet
   */
  def endpointActorName = "Endpoint-" + java.lang.Thread.currentThread.getStackTrace()(1).getMethodName + System.nanoTime()
}