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

package co.vaughnvernon.reactiveenterprise.messageexpiration

import java.util.concurrent.TimeUnit
import java.util.Date
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._
import ExecutionContext.Implicits.global
import akka.actor._
import co.vaughnvernon.reactiveenterprise._

trait ExpiringMessage {
  val occurredOn = System.currentTimeMillis()
  val timeToLive: Long
  
  def isExpired: Boolean = {
    val elapsed = System.currentTimeMillis() - occurredOn
    
    elapsed > timeToLive
  }
}

case class PlaceOrder(id: String, itemId: String, price: Double, timeToLive: Long) extends ExpiringMessage

object MessageExpirationDriver extends CompletableApp(3) {
  val purchaseAgent = system.actorOf(Props[PurchaseAgent], "purchaseAgent")
  val purchaseRouter = system.actorOf(Props(classOf[PurchaseRouter], purchaseAgent), "purchaseRouter")
  
  purchaseRouter ! PlaceOrder("1", "11", 50.00, 1000)
  purchaseRouter ! PlaceOrder("2", "22", 250.00, 100)
  purchaseRouter ! PlaceOrder("3", "33", 32.95, 10)
  
  awaitCompletion
  println("MessageExpiration: is completed.")
}

class PurchaseRouter(purchaseAgent: ActorRef) extends Actor {
  val random = new Random((new Date()).getTime)
  
  def receive = {
    case message: Any =>
      val millis = random.nextInt(100) + 1
      println(s"PurchaseRouter: delaying delivery of $message for $millis milliseconds")
      val duration = Duration.create(millis, TimeUnit.MILLISECONDS)
      context.system.scheduler.scheduleOnce(duration, purchaseAgent, message)
  }
}

class PurchaseAgent extends Actor {
  def receive = {
    case placeOrder: PlaceOrder =>
      if (placeOrder.isExpired) {
        context.system.deadLetters ! placeOrder
        println(s"PurchaseAgent: delivered expired $placeOrder to dead letters")
      } else {
        println(s"PurchaseAgent: placing order for $placeOrder")
      }
      
      MessageExpirationDriver.completedStep()
      
    case message: Any =>
      println(s"PurchaseAgent: received unexpected: $message")
  }
}
