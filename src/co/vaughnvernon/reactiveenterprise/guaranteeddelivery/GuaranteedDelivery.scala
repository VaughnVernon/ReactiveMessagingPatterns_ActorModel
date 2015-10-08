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

package co.vaughnvernon.reactiveenterprise.guaranteeddelivery

import akka.actor._
import akka.persistence._
import co.vaughnvernon.reactiveenterprise.CompletableApp
import java.util.UUID
import java.util.Date

import akka.persistence.ConfirmablePersistent
import akka.persistence.Deliver

object GuaranteedDeliveryDriver extends CompletableApp(2) {
  val analyzer1 =
    system.actorOf(Props[OrderAnalyzer], "orderAnalyzer1")

  val orderProcessor1 =
    system.actorOf(Props(classOf[OrderProcessor], analyzer1.path), "orderProcessor1")
    
  val orderId = new Date().getTime.toString
  println(s"Processing: $orderId")
  orderProcessor1 ! Persistent(ProcessOrder(orderId, "Details..."))
  
  awaitCompletion
}

case class ProcessOrder(orderId: String, details: String)

class OrderProcessor(orderAnalyzer: ActorPath) extends Processor {
  val channel =
    context.actorOf(
        Channel.props(),
        s"${self.path.name}-channel")

  override def preStart() = {
    self ! Recover(replayMax=0L)
  }
  
  def receive = {
    case message @ Persistent(actualMessage, sequenceNumber) =>
      print(s"Handling persisted: $sequenceNumber: ")
      actualMessage match {
        case processOrder: ProcessOrder =>
          println(s"ProcessOrder: $processOrder")
          channel ! Deliver(message, orderAnalyzer)
          GuaranteedDeliveryDriver.completedStep
        case unknown: Any =>
          println(s"Unknown: $unknown")
      }
    case PersistenceFailure(actualMessage, sequenceNumber, cause) =>
      println(s"Handling failed persistent: actualMessage")
      GuaranteedDeliveryDriver.completedStep
    case non_persisted: Any =>
      println(s"Handling non-persistent: $non_persisted")
      GuaranteedDeliveryDriver.completedStep
  }
}

class OrderAnalyzer extends Actor {
  def receive = {
    case confirmable @ ConfirmablePersistent(
        actualMessage, sequenceNumber, redeliveries) =>

      println(s"OrderAnalyzer: $actualMessage")
      confirmable.confirm
      GuaranteedDeliveryDriver.completedStep
  }
}
