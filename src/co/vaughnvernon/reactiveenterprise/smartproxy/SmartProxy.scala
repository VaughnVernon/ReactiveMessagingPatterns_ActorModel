//   Copyright 2012,2015 Vaughn Vernon
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package co.vaughnvernon.reactiveenterprise.smartproxy

import akka.actor._
import co.vaughnvernon.reactiveenterprise.CompletableApp

object SmartProxyDriver extends CompletableApp(6) {
  val serviceProvider = system.actorOf(
      Props[ServiceProvider],
      "serviceProvider")

  val proxy = system.actorOf(
      Props(classOf[ServiceProviderProxy], serviceProvider),
      "proxy")
  
  val requester1 = system.actorOf(
      Props(classOf[ServiceRequester], proxy),
      "requester1")
      
  val requester2 = system.actorOf(
      Props(classOf[ServiceRequester], proxy),
      "requester2")
      
  val requester3 = system.actorOf(
      Props(classOf[ServiceRequester], proxy),
      "requester3")

  requester1 ! RequestService(ServiceRequestOne("1"))
  requester2 ! RequestService(ServiceRequestTwo("2"))
  requester3 ! RequestService(ServiceRequestThree("3"))

  awaitCompletion
  
  println("SmartProxy: completed.")
}

case class RequestService(service: ServiceRequest)

class ServiceRequester(serviceProvider: ActorRef) extends Actor {
  def receive = {
    case request: RequestService =>
      println(s"ServiceRequester: ${self.path.name}: $request")
      serviceProvider ! request.service
      SmartProxyDriver.completedStep
    case reply: Any =>
      println(s"ServiceRequester: ${self.path.name}: $reply")
      SmartProxyDriver.completedStep
  }
}

class ServiceProviderProxy(serviceProvider: ActorRef) extends Actor {
  val requesters = scala.collection.mutable.Map[String, ActorRef]()
  
  def receive = {
    case request: ServiceRequest =>
      requesters(request.requestId) = sender
      serviceProvider ! request
      analyzeRequest(request)
    case reply: ServiceReply =>
      val sender = requesters.remove(reply.replyId)
      if (sender.isDefined) {
    	analyzeReply(reply)
        sender.get ! reply
      }
  }
  
  def analyzeReply(reply: ServiceReply) = {
    println(s"Reply analyzed: $reply")
  }
  
  def analyzeRequest(request: ServiceRequest) = {
    println(s"Request analyzed: $request")
  }
}

trait ServiceRequest {
  def requestId: String
}

case class ServiceRequestOne(requestId: String) extends ServiceRequest
case class ServiceRequestTwo(requestId: String) extends ServiceRequest
case class ServiceRequestThree(requestId: String) extends ServiceRequest

trait ServiceReply {
  def replyId: String
}

case class ServiceReplyOne(replyId: String) extends ServiceReply
case class ServiceReplyTwo(replyId: String) extends ServiceReply
case class ServiceReplyThree(replyId: String) extends ServiceReply

class ServiceProvider extends Actor {
  def receive = {
    case one: ServiceRequestOne =>
      sender ! ServiceReplyOne(one.requestId)
    case two: ServiceRequestTwo =>
      sender ! ServiceReplyTwo(two.requestId)
    case three: ServiceRequestThree =>
      sender ! ServiceReplyThree(three.requestId)
  }
}
