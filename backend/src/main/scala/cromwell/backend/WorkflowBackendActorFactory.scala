package cromwell.backend

import akka.actor.{ActorRef, ActorSystem}
import cromwell.backend.model.WorkflowDescriptor

trait WorkflowBackendActorFactory {

  /**
    * Returns a backend actor instance based on the init class, Cromwell actor system and specific workflow.
    *
    * @param initClass          Initialization class for the specific backend.
    * @param actorSystem        Cromwell Engine actor system.
    * @param workflowDescriptor Needed data to be able to execute a workflow.
    * @return An actor of type BackendActor.
    */
  //TODO: Add backend configuration arg.
  //TODO: discuss about actorSystem... should be a backend actor system or the same from engine.
  def getBackend(initClass: String, actorSystem: ActorSystem, workflowDescriptor: WorkflowDescriptor): ActorRef

}